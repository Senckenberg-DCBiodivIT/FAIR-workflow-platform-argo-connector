import logging
import os
from pathlib import Path
import tempfile
import magic
import cordra
from typing import Any, Generator
import json_merge_patch

logger = logging.getLogger("uvicorn.error")

def check_health(host: str, user: str, password: str) -> bool|str:
    try:
        schemas = cordra.CordraObject.find(host=host, username=user, password=password, verify=False, query="type:Schema")
        if schemas.get("size", 0) == 0:
            return "No schemas found"
    except Exception as e:
        return str(e)

    return True

def create_dataset_from_workflow_artifacts(host: str, user: str, password: str, wfl: dict[str: Any], artifact_stream_iterator: Generator, file_max_size: int = 100*1024*1024) -> str:
    upload_kwargs = {
        "host": host,
        "username": user,
        "password": password,
        "verify": False
    }

    created_ids = {}
    try:
        # add authors
        logger.debug("Creating authors")
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

        logger.debug("Create Dataset")
        properties = {
            "name": wfl["metadata"].get("annotations", {}).get("workflows.argoproj.io/title", wfl["metadata"]["name"]),
            "description": wfl["metadata"].get("annotations", {}).get("workflows.argoproj.io/description", None),
            "author": [author1["@id"], author2["@id"]],
            "hasPart": [id for id in created_ids if created_ids[id] == "FileObject"],
        }

        # TODO derive these from the workflow somehow
        action = None
        if wfl["metadata"]["name"].startswith("modgp-"):
            logger.info("This is a modgp workflow. Applying hardcoded values")

            # create action
            logger.debug("Create SoftwareApplication")
            instrument = cordra.CordraObject.create(
                obj_type="SoftwareApplication",
                obj_json={
                    "name": "ModGP",
                    "identifier": "https://github.com/BioDT/uc-CWR"
                },
                **upload_kwargs
            )
            created_ids[instrument["@id"]] = "SoftwareApplication"

            logger.debug("Create CreateAction")
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

            species = next(filter(lambda x: x["name"] == "species", wfl["spec"]["arguments"]["parameters"]))["value"]
            dataset_patch = {
                "name": f"Species distribution models for {species}",
                "description": f"Species distribution model calculated with ModGP for {species}",
                "keywords": ["GBIF", "Occurrence", "Biodiversity", "Observation", "ModGP", "SDM"],
                "license": "https://spdx.org/licenses/CC-BY-SA-2.0",
                "mentions": [action["@id"]],
            }
            properties = json_merge_patch.merge(properties, dataset_patch)

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
