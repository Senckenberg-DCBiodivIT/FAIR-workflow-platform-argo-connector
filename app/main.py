import json

import argo_workflows.exceptions
import yaml
from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, UploadFile
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic_settings import BaseSettings, SettingsConfigDict
from app import cordra, argo
from fastapi.responses import JSONResponse
import logging
from typing import Annotated

class Settings(BaseSettings):
    auth_username: str | None = None
    auth_password: str | None = None

    argo_base_url: str
    argo_token: str
    argo_default_namespace: str = "argo"

    cordra_max_file_size: int = 100 * 1024 * 1024
    cordra_base_url: str
    cordra_user: str
    cordra_password: str

    root_path: str|None = None  # might be behind a proxy. This would be the prefix then.

    model_config = SettingsConfigDict(env_file=".env")  # for dev env

settings = Settings()
app = FastAPI(title="CWR Argo Connector", root_path=settings.root_path)
logger = logging.getLogger("uvicorn.error")

security = HTTPBasic()
if settings.auth_username is None or settings.auth_password is None:
    logger.warning("No authentication enabled!")

def check_auth(credentials: Annotated[HTTPBasicCredentials, Depends(security)]):
    if settings.auth_username is None or settings.auth_password is None:
        return
    if not (credentials.username == settings.auth_username and credentials.password == settings.auth_password):
        raise HTTPException(status_code=401, detail="Incorrect username or password")

def process_workflow(name: str, namespace: str):
    logger.info(f"Ingesting {namespace}/{name}")
    wfl = argo.get_workflow_information(settings.argo_base_url, settings.argo_token, namespace, name, verify_cert=False)
    artifacts = argo.parse_artifact_list(wfl)

    reconstructed_wfl = argo.reconstruct_workflow_from_workflowinfo(wfl)

    logger.info(f"Found {len(artifacts)} artifacts to process")
    artifact_stream_iterator = argo.artifact_reader(
        host=settings.argo_base_url,
        token=settings.argo_token,
        namespace=namespace,
        workflow_name=name,
        artifact_list=artifacts,
        verify_cert=False
    )
    cordra.create_dataset_from_workflow_artifacts(
        host=settings.cordra_base_url,
        user=settings.cordra_user,
        password=settings.cordra_password,
        wfl=wfl,
        artifact_stream_iterator=artifact_stream_iterator,
        reconstructed_wfl=reconstructed_wfl,
        file_max_size=settings.cordra_max_file_size
    )
    logger.info(f"Successfully ingested {namespace}/{name}")

@app.get("/")
def healthcheck():
    cordra_health = cordra.check_health(settings.cordra_base_url, settings.cordra_user, settings.cordra_password)
    argo_health = argo.check_health(settings.argo_base_url, settings.argo_token, settings.argo_default_namespace, verify_cert=False)
    response = {
        "cordra_connection": cordra_health,
        "argo_connection": argo_health
    }
    if cordra_health != True or argo_health != True:
        raise HTTPException(status_code=500, detail=response)
    else:
        return JSONResponse(response)

@app.get("/notify/{namespace}/{name}", dependencies=[Depends(check_auth)])
def notify(namespace: str, name: str, background_tasks: BackgroundTasks):

    # Sanity check. Is this a valid workflow
    try:
        logger.info(f"Retrieving Workflow information for {namespace}/{name}")
        wfl = argo.get_workflow_information(
            host=settings.argo_base_url,
            token=settings.argo_token,
            namespace=namespace,
            workflow_name=name,
            verify_cert=False
        )
    except argo_workflows.exceptions.NotFoundException:
        raise HTTPException(status_code=404, detail="Workflow not found")

    # check if workflow is finished
    unsucceeded_nodes = []
    for node_id in wfl["status"]["nodes"]:
        node = wfl["status"]["nodes"][node_id]
        if "onExit" in node["name"]: continue # ignore exit nodes
        if node["phase"] != "Succeeded":
            unsucceeded_nodes.append(node["name"])

    if wfl["status"]["phase"] != "Succeeded":
        if len(unsucceeded_nodes) > 0:
            raise HTTPException(status_code=400, detail=f"Workflow did not succeed (unsuccessful nodes: {unsucceeded_nodes})")
        else:
            logger.info("Workflow still running, but only on exit handler. Continue processing")

    # are there any artifacts to process?
    artifacts = argo.parse_artifact_list(wfl)
    if len(artifacts) == 0:
        return HTTPException(status_code=400, detail="No artifacts found")

    logger.info(f"Starting background task to process {namespace}/{name}")
    background_tasks.add_task(process_workflow, name, namespace)

    return JSONResponse(status_code=202, content={
        "status": "accepted",
        "workflow_status": wfl["status"]["phase"],
        "workflow_name": wfl["metadata"]["name"],
        "workflow_namespace": wfl["metadata"]["namespace"],
        "artifacts": [{"node_id": node_id, "path": path} for (node_id, _, path) in artifacts],
    })

@app.post("/workflow/check", dependencies=[Depends(check_auth)])
async def check_workflow(file: UploadFile):
    content = await file.read()
    content = yaml.load(content, Loader=yaml.CLoader)
    try:
        return argo.verify(settings.argo_base_url, settings.argo_token, content, namespace=content["metadata"].get("namespace", settings.argo_default_namespace), verify_cert=False)
    except argo_workflows.exceptions.ApiException as e:
        raise HTTPException(status_code=400, detail=json.loads(e.body))


@app.post("/workflow/submit", dependencies=[Depends(check_auth)])
async def submit(file: UploadFile, dryRun: bool = False):
    logger.info("Linting workflow...")
    checked_workflow = await check_workflow(file)
    logger.info(f"Submitting workflow (dryRun:{dryRun})")
    try:
        return argo.submit(settings.argo_base_url, settings.argo_token, checked_workflow, namespace=checked_workflow["metadata"].get("namespace", settings.argo_default_namespace), dry_run=dryRun, verify_cert=False)
    except argo_workflows.exceptions.ApiException as e:
        raise HTTPException(status_code=400, detail=json.loads(e.body))



