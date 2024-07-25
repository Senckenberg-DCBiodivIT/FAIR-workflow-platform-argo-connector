from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic_settings import BaseSettings, SettingsConfigDict
from app import cordra, argo
from fastapi.responses import JSONResponse
import logging

class Settings(BaseSettings):
    argo_base_url: str
    argo_token: str

    cordra_base_url: str
    cordra_user: str
    cordra_password: str

    model_config = SettingsConfigDict(env_file=".env")  # for dev env

settings = Settings()
app = FastAPI(title="CWR Argo Connector")
logger = logging.getLogger("uvicorn.error")

def process_workflow(name: str, namespace: str):
    logger.info(f"Ingesting {namespace}/{name}")
    wfl = argo.get_workflow_information(settings.argo_base_url, settings.argo_token, namespace, name, verify_cert=False)
    artifacts = argo.get_artifact_list(wfl)

    logger.info(f"Found {len(artifacts)} artifacts to process")
    artifact_stream_iterator = argo.artifact_reader(
        argo_url=settings.argo_base_url,
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
        artifact_stream_iterator=artifact_stream_iterator
    )
    logger.info(f"Successfully ingested {namespace}/{name}")

@app.get("/notify/{namespace}/{name}")
def notify(namespace: str, name: str, background_tasks: BackgroundTasks):

    # Sanity check. Is this a valid workflow
    wfl = argo.get_workflow_information(settings.argo_base_url, settings.argo_token, namespace, name, verify_cert=False)
    if wfl["status"]["phase"] != "Succeeded":
        raise HTTPException(status_code=400, detail="Workflow did not succeed")

    # are there any artifacts to process?
    artifacts = argo.get_artifact_list(wfl)
    if len(artifacts) == 0:
        return HTTPException(status_code=400, detail="No artifacts found")

    background_tasks.add_task(process_workflow, name, namespace)

    return JSONResponse(status_code=202, content={
        "status": "accepted",
        "workflow_status": wfl["status"]["phase"],
        "workflow_name": wfl["metadata"]["name"],
        "workflow_namespace": wfl["metadata"]["namespace"],
        "artifacts": [{"node_id": node_id, "path": path} for (node_id, _, path) in artifacts],
    })
