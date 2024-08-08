import logging
from datetime import datetime
import os
from pathlib import Path
import tempfile
import magic
import cordra
from typing import Any, Generator
import json_merge_patch
import yaml

logger = logging.getLogger("uvicorn.error")

def check_health(host: str, user: str, password: str) -> bool|str:
    try:
        schemas = cordra.CordraObject.find(host=host, username=user, password=password, verify=False, query="type:Schema")
        if schemas.get("size", 0) == 0:
            return "No schemas found"
    except Exception as e:
        return str(e)

    return True

def create_dataset_from_workflow_artifacts(host: str, user: str, password: str, wfl: dict[str: Any], artifact_stream_iterator: Generator, reconstructed_wfl: dict[str: Any], file_max_size: int = 100*1024*1024) -> str:
    upload_kwargs = {
        "host": host,
        "username": user,
        "password": password,
        "verify": False
    }

    created_ids = {}
    try:
        annotations = reconstructed_wfl["metadata"]["annotations"]
        create_action_author = None
        for key, value in annotations.items():
            if key.startswith("argo-connector/submitterId"):
                number = key.split("argo-connector/submitterId")[1]

                author_id = "https://orcid.org/" + wfl["metadata"]["annotations"][f"argo-connector/submitterId{number}"]
                author_properties = {"identifier": author_id}
                if f"argo-connector/submitterName{number}" in wfl["metadata"]["annotations"]:
                    author_properties["name"] = wfl["metadata"]["annotations"][f"argo-connector/submitterName{number}"]
                author = cordra.CordraObject.create(obj_type="Person", obj_json=author_properties, **upload_kwargs)
                if number == "1":
                    create_action_author = author
                created_ids[author["@id"]] = "Person"

        # upload files
        logger.debug("Creating file objects")
        for file_name, content_iterator in artifact_stream_iterator:

            logger.debug("Creating FileObject from " + file_name)
            relative_path = file_name

            # Write file content into temporary file
            # Streaming the file directly into cordra would be better, but this didnt work for me.
            with tempfile.NamedTemporaryFile(delete=True,
                                             prefix=f"argo-artifact-tmp-{os.path.basename(file_name)}-") as tmp_file:
                try:
                    logger.debug("Downloading content to temp file: " + tmp_file.name)
                    for chunk in content_iterator:
                        if chunk:
                            tmp_file.write(chunk)
                except Exception as e:
                    logger.error("Failed to download file content: " + str(e))
                    content_iterator.close()
                    raise e
                tmp_file.flush()
                file_size = Path(tmp_file.name).stat().st_size

                logger.debug(f"Download done ({file_size / 1024 / 1024:.2f} MB)")

                # Cordra has issues with huge files
                if file_size > file_max_size:
                    logger.warning(f"File size is {file_size / 1024 / 1024:.2f} MB, which is too large to upload. Skipping...")
                    continue

                # figure out file encoding
                try:
                    encoding_format = magic.from_file(tmp_file.name, mime=True)
                    logger.debug("Infered encoding format: " + encoding_format)
                except magic.MagicException as e:
                    logger.warning(f"Failed to get encoding format for {file_name}")
                    encoding_format = None

                # write object
                logger.debug("Sending file to cordra")
                file_obj = cordra.CordraObject.create(
                    obj_type="FileObject",
                    obj_json={
                        "name": os.path.basename(relative_path),
                        "contentSize": Path(tmp_file.name).stat().st_size,
                        "encodingFormat": encoding_format,
                        "contentUrl": relative_path,
                    },
                    payloads={relative_path: (relative_path, open(tmp_file.name, "rb"))},
                    **upload_kwargs
                )
                logger.debug("File ingested")
            created_ids[file_obj["@id"]] = "FileObject"

        logger.debug("Create workflow and action parameters")
        parameters = reconstructed_wfl.get("spec", {}).get("arguments", {}).get("parameters", [])
        for parameter in parameters:
            parameter_json = {
                "name": parameter["name"],
            }
            if "description" in parameter:
                parameter_json["description"] = parameter["description"]
            parameter_obj = cordra.CordraObject.create(
                obj_type="FormalParameter",
                obj_json=parameter_json,
                **upload_kwargs
            )
            created_ids[parameter_obj["@id"]] = "FormalParameter"
            if "value" in parameter:
                parameter_json["value"] = parameter["value"]
                property_obj = cordra.CordraObject.create(
                    obj_type="PropertyValue",
                    obj_json=parameter_json,
                    **upload_kwargs
                )
                created_ids[property_obj["@id"]] = "PropertyValue"

        logger.debug("Create workflow")
        with tempfile.NamedTemporaryFile(delete=True,
                                         prefix=f"argo-workflow-tmp-") as tmp_file:
            yaml.dump(reconstructed_wfl, open(tmp_file.name, "w"), indent=2)
            tmp_file.flush()
            wfl_obj = cordra.CordraObject.create(
                obj_type="Workflow",
                obj_json={
                    "name": "workflow.yaml",
                    "contentSize": Path(tmp_file.name).stat().st_size,
                    "encodingFormat": "text/yaml",
                    "contentUrl": "workflow.yaml",
                    "description": "Argo workflow definition",
                    "programmingLanguage": "https://argoproj.github.io/workflows",
                    "input": [id for id, object_type in created_ids.items() if object_type == "FormalParameter"],
                },
                payloads={"workflow.yaml": ("workflow.yaml", open(tmp_file.name, "rb"))},
                **upload_kwargs
            )
            logger.debug("Workflow ingested")
            created_ids[wfl_obj["@id"]] = "Workflow"


        logger.debug("Create CreateAction")
        date_format = "%Y-%m-%dT%H:%M:%SZ"
        start_time = datetime.strptime(wfl["status"]["startedAt"], date_format)
        end_time = wfl["status"].get("finishedAt", None)
        if end_time is not None:
            end_time = datetime.strptime(end_time, date_format)
        else:
            for node_name in wfl["status"]["nodes"]:
                node_end_time = wfl["status"]["nodes"][node_name].get("finishedAt", None)
                if node_end_time is not None:
                    node_end_time = datetime.strptime(node_end_time, date_format)
                    if end_time is None or node_end_time > end_time:
                        end_time = node_end_time

        action_properties = {
            "result": [id for id in created_ids if created_ids[id] == "FileObject"],
            "startTime": start_time.strftime(date_format),
            "endTime": end_time.strftime(date_format),
            "instrument": wfl_obj["@id"],
            "object": [id for id, object_type in created_ids.items() if object_type == "PropertyValue"],
        }
        if create_action_author is not None:
            action_properties["agent"] = create_action_author["@id"]
        action = cordra.CordraObject.create(
            obj_type="CreateAction",
            obj_json=action_properties,
            **upload_kwargs
        )
        created_ids[action["@id"]] = "CreateAction"

        properties = {
            "name": wfl["metadata"].get("annotations", {}).get("workflows.argoproj.io/title", wfl["metadata"]["name"]),
            "description": wfl["metadata"].get("annotations", {}).get("workflows.argoproj.io/description", None),
            "author": [id for id in created_ids if created_ids[id] == "Person"],
            "hasPart": [id for id in created_ids if created_ids[id] == "FileObject" or created_ids[id] == "Workflow"],
            "mentions": [action["@id"]],
            "mainEntity": wfl_obj["@id"]
        }

        # TODO derive these from the workflow somehow
        if wfl["metadata"]["name"].startswith("modgp-"):
            logger.info("This is a modgp workflow. Applying hardcoded values")

            # create action
            logger.debug("Create SoftwareApplication")
            # instrument = cordra.CordraObject.create(
            #     obj_type="SoftwareApplication",
            #     obj_json={
            #         "name": "ModGP",
            #         "identifier": "https://github.com/BioDT/uc-CWR"
            #     },
            #     **upload_kwargs
            # )
            # created_ids[instrument["@id"]] = "SoftwareApplication"
            #
            # logger.debug("Update action for instrument")
            # cordra.CordraObject.update(
            #     obj_id=action["@id"],
            #     obj_json=json_merge_patch.merge(action, {"instrument": instrument["@id"]}),
            #     **upload_kwargs
            # )

            species = next(filter(lambda x: x["name"] == "species", wfl["spec"]["arguments"]["parameters"]))["value"]
            dataset_patch = {
                "name": f"Species distribution models for {species}",
                "description": f"Species distribution model calculated with ModGP for {species}",
                "keywords": ["GBIF", "Occurrence", "Biodiversity", "Observation", "ModGP", "SDM"],
                "license": "https://spdx.org/licenses/CC-BY-SA-2.0",
            }
            properties = json_merge_patch.merge(properties, dataset_patch)

        logger.debug("Create Dataset")
        dataset = cordra.CordraObject.create(obj_type="Dataset", obj_json=properties, **upload_kwargs)
        created_ids[dataset["@id"]] = "Dataset"

        # Update files parfOf/resultOf to point to dataset/action
        logger.debug("Updating files backref to dataset/action")
        for cordra_id in [id for id in created_ids if created_ids[id] == "FileObject"]:
            obj = cordra.CordraObject.read(obj_id=cordra_id, **upload_kwargs)
            if ("partOf" not in obj) or (obj["partOf"] is None):
                obj["partOf"] = [dataset["@id"]]
            if action is not None:
                obj["resultOf"] = action["@id"]
            cordra.CordraObject.update(obj_id=cordra_id, obj_json=obj, **upload_kwargs)

        logger.info(f"Dataset ingested. Cordra ID: {dataset['@id']} ({host}/objects/{dataset['@id']})")
        return dataset["@id"]
    except Exception as e:
        print(f"Failed to create corda dataset: {type(e)} {str(e)}. Cleaning up uploaded objects")
        for cordra_id in created_ids:
            cordra.CordraObject.delete(obj_id=cordra_id, **upload_kwargs)
        raise e
