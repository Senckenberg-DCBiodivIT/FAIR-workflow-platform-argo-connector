#!/usr/bin/env python

import argo_workflows
from argo_workflows.api import workflow_service_api
import requests
import os
import yaml
from bs4 import BeautifulSoup
import urllib.parse
from pathlib import Path
import tempfile
import magic
import cordra

import urllib3
urllib3.disable_warnings()

def build_argo_client(url: str, token: str, verify_cert: bool = True):
    config = argo_workflows.Configuration(host=url,
                                          api_key_prefix={"BearerToken": f"Bearer {token}"},
                                          api_key={"BearerToken": "Bearer"})
    config.verify_ssl = verify_cert
    return argo_workflows.ApiClient(config)

def get_workflow_information(argo_url: str, argo_token: str, namespace: str, workflow_name: str, verify_cert: bool = True):
    client = build_argo_client(argo_url, argo_token, verify_cert=verify_cert)
    api = workflow_service_api.WorkflowServiceApi(client)
    wfl = api.get_workflow(namespace, workflow_name, _check_return_type=False).to_dict()
    return wfl

def reconstruct_workflow(wfl):
    workflow_reconstructed = dict(
        kind="Workflow",
        spec=wfl["spec"],
    )
    templateSpec = wfl["status"]["storedWorkflowTemplateSpec"]
    for key in templateSpec:
        workflow_reconstructed["spec"][key] = templateSpec[key]
    
    # remove te template reference. we export the whole workflow
    del workflow_reconstructed["spec"]["workflowTemplateRef"]
    return workflow_reconstructed

def get_artifact_list(wfl):
    # find the last node in the node list
    artifacts_list = []
    for node_name in wfl["status"]["nodes"]:
        node = wfl["status"]["nodes"][node_name]

        if not "outputs" in node or not "artifacts" in node["outputs"]:
            continue

        artifacts = node["outputs"]["artifacts"]
        for artifact in artifacts:
            # ignore artifacts with a key outside of this wfl, since these are for caching
            if wfl["metadata"]["name"] not in artifact["s3"]["key"]:
                continue

            # ignore artifacts that are GCedd or planned to be GCed
            # these are intended for communication between steps
            if "deleted" in artifact and artifact["deleted"]:
                continue
            if "artifactGC" in artifact and artifact["artifactGC"]["strategy"] != "Never":
                continue

            if artifact["name"] == "main-logs":
                artifacts_list.append((node_name, artifact["name"], "main.log"))
            else:
                artifacts_list.append((node_name, artifact["name"], artifact["path"]))
        
    return artifacts_list

def recursive_artifact_reader(url: str, argo_token: str, path: str, verify_cert: bool = True, chunk_size=1024*1024):
    headers = {"Authorization": f"Bearer {argo_token}"}

    # check header
    head = requests.head(url, verify=verify_cert, headers=headers)
    if head.status_code != 200:
        head.raise_for_status()
    if "Content-Disposition" in head.headers:  # it is a file
        print(f"Yielding file from  {url}")
        download_req = requests.get(url, verify=verify_cert, headers=headers, stream=True)
        download_req.raise_for_status()
        yield path, download_req.iter_content(chunk_size=chunk_size)
    else:
        print("Downloading directory recursively: " + url)
        content = requests.get(url, verify=verify_cert, headers=headers).content
        soup = BeautifulSoup(content, features="html.parser")
        for link in soup.find_all("a"):
            href = link.get("href")
            if href == "..":
                continue
            new_url = urllib.parse.urljoin(url + "/", href)
            new_path = os.path.join(path, href)
            yield from recursive_artifact_reader(new_url, argo_token, new_path, verify_cert, chunk_size)

def artifact_reader(argo_url: str, token: str, namespace: str, workflow_name: str, artifact_list: list[tuple[str, str, str]] , verify_cert: bool = True):
    for (node_id, artifact_name, file_path) in artifact_list:
        # build my own http request because the API submits "workflow" as discriminator where it should be "workflows".
        url = f"{argo_url}/artifact-files/{namespace}/workflows/{workflow_name}/{node_id}/outputs/{artifact_name}"

        # build relative path for artifacts
        if file_path.startswith("/"):
            file_path = file_path[1:]
        path = os.path.join(node_id, file_path)

        yield from recursive_artifact_reader(url, token, path, verify_cert)


def upload_dataset_to_cordra(host, user, password, wfl_path, artifact_stream_iterator):
    upload_kwargs = {
        "host": host,
        "username": user,
        "password": password,
        "verify": False
    }

    created_ids = {}
    try:
        # add authors
        print("Creating authors")
        author1 = cordra.CordraObject.create(obj_type="Person", obj_json={
            "name": "Daniel Bauer",
            "identifier": "https://orcid.org/0000-0001-9447-460X",
        }, **upload_kwargs)
        created_ids[author1["@id"]] = "Person"
        
        author2 = cordra.CordraObject.create(obj_type="Person", obj_json={
            "name": "Erik Kusch",
            "identifier": "https://orcid.org/0000-0002-4984-7646"
        }, **upload_kwargs)
        created_ids[author2["@id"]] = "Person"

        # upload files
        print("Creating file objects")
        for file_name, content_iterator in artifact_stream_iterator:

                print("Creating FileObject " + file_name)
                relative_path = file_name

                # Write file content into temporary file
                # Streaming the file directly into cordra would be better, but this didnt work for me.
                with tempfile.NamedTemporaryFile(delete=True, prefix=f"argo-artifact-tmp-{os.path.basename(file_name)}-") as tmp_file:
                    print("Downloading content to temp file: " + tmp_file.name)
                    try:
                        for chunk in content_iterator:
                            if chunk:
                                tmp_file.write(chunk)
                    except Exception as e:
                        content_iterator.close()
                        raise e
                    tmp_file.flush()
                    file_size = Path(tmp_file.name).stat().st_size

                    print(f"Data written to temp file {tmp_file.name} ({file_size / 1024 / 1024:.2f} MB)")
                    
                    # Cordra has issues with huge files
                    if file_size / (1024*1024) > 1000:
                        print(f"File size is {file_size / 1024 / 1024:.2f} MB, which is too large to upload. Skipping...")
                        continue
                    
                    # figure out file encoding
                    try:
                        encodingFormat = magic.from_file(tmp_file.name, mime=True)
                        print("Infered encoding format: " + encodingFormat)
                    except magic.MagicException as e:
                        print(f"Failed to get encoding format for {file_name}")
                        encodingFormat = None

                    # write object
                    file_obj = cordra.CordraObject.create(
                        obj_type="FileObject",
                        obj_json={
                            "name": os.path.basename(relative_path),
                            "contentSize": Path(tmp_file.name).stat().st_size,
                            "encodingFormat": encodingFormat,
                            "contentUrl": relative_path,
                        },
                        payloads={relative_path: (relative_path, open(tmp_file.name, "rb"))},
                        **upload_kwargs
                    )
                created_ids[file_obj["@id"]] = "FileObject"
                break  ## TODO remove to process multiple files

        # create action
        # TODO use workflow as action instead of software application
        print("Create Action")
        instrument = cordra.CordraObject.create(
            obj_type="SoftwareApplication",  
            obj_json={
                "name": "ModGP",
                "identifier": "https://github.com/BioDT/uc-CWR"
            },
            **upload_kwargs
        )
        created_ids[instrument["@id"]] = "SoftwareApplication"

        action = cordra.CordraObject.create(
            obj_type="CreateAction",
            obj_json={
                "agent": author1["@id"],
                "result": [id for id in created_ids if created_ids[id] == "FileObject"],
                "instrument": instrument["@id"]
            },
            **upload_kwargs
        )
        created_ids[action["@id"]] = "CreateAction"

        print("Create Dataset")
        # TODO derive keywords, name and description from workflow input?
        dataset = cordra.CordraObject.create(obj_type="Dataset",obj_json={
            "name": "Species distribution models for <Enter Species>",
            "description": "ModGP workflow output for <Enter Species>",
            "keywords": ["GBIF", "Occurrence", "Biodiversity", "Observation", "ModGP", "SDM"],
            "license": "https://spdx.org/licenses/CC-BY-SA-2.0",
            "author": [author1["@id"], author2["@id"]],
            "hasPart": [id for id in created_ids if created_ids[id] == "FileObject"],
            "mentions": [action["@id"]],
        }, **upload_kwargs)
        created_ids[dataset["@id"]] = "Dataset"

        # Update files parfOf/resultOf to point to dataset/action
        print("Updating files backref to dataset/action")
        for id in [id for id in created_ids if created_ids[id] == "FileObject"]:
            obj = cordra.CordraObject.read(obj_id=id, **upload_kwargs)
            if ("partOf" not in obj) or (obj["partOf"] is None):
                obj["partOf"] = [ dataset["@id"] ]
            obj["resultOf"] = action["@id"]
            cordra.CordraObject.update(obj_id=id, obj_json=obj, **upload_kwargs)

    except Exception as e:
        print(f"Failed to create corda dataset: {type(e)} {str(e)}. Cleaning up uploaded objects")
        for id in created_ids:
            cordra.CordraObject.delete(obj_id=id, **upload_kwargs)
        
if __name__ == "__main__":
    token = "TODO"
    argo_url = "https://217.71.193.22:443"
    verify_cert = False

    namespace = "argo"
    workflow_name = "modgp-trtpg"
    # workflow_name = "modgp-crklg"

    # cordra_host = "https://localhost:8443"
    # cordra_user = "admin"
    # cordra_password = "password"
    cordra_host = "https://217.71.193.143:8443"
    cordra_user = "admin"
    cordra_password = "TODO"

    wfl = get_workflow_information(argo_url, token, namespace, workflow_name, verify_cert)

    if wfl is None or wfl["status"]["phase"] != "Succeeded":
        print("Artifact export failed for workflow: " + workflow_name + ": Workflow missing or incomplete")
    else:
        tempdir = "./tmp"
        if not os.path.exists(tempdir):
            os.mkdir(tempdir)

        wfl_file = reconstruct_workflow(wfl)
        wfl_path = os.path.join(tempdir, "workflow.yaml")
        with open(wfl_path, "w") as f:
            f.write(yaml.dump(wfl_file))

        artifact_list = get_artifact_list(wfl)

        artifact_stream_iterator = artifact_reader(
            argo_url, token, namespace, workflow_name, artifact_list, verify_cert)
        upload_dataset_to_cordra(cordra_host, cordra_user, cordra_password, wfl_path, artifact_stream_iterator)
