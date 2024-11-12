# Argo-connector

A connector that can start argo workflows from a submitted workflow file and 
push resulting artifacts from argo to cordra to store them as digital objects.

This is part of the [FAIR workflow platform](https://github.com/dnlbauer/FAIR-workflow-platform).

## Build

```bash
docker build . argo-connector .
```

## Run with docker

```bash
docker run -d -p 8000:8000 --env-file .env argo-connector
```

Make sure to set the neccessary environment variables:

| Variable name | Description | Default |
|--|--|--|
| ARGO_BASE_URL	| Base URL of the Argo workflow engine | |
| ARGO_TOKEN | Authentication token for Argo | |
| CORDRA_BASE_URL | Base URL of the Cordra instance | |
| CORDRA_USER | Username for Cordra authentication | |
| CORDRA_PASSWORD | Password for Cordra authentication | |
| ARGO_DEFAULT_NAMESPACE | Default namespace for Argo workflows | "argo" |
| CORDRA_MAX_FILE_SIZE | Maximum file size for Cordra uploads (in bytes). Some Cordra backends like S3 have a fixed file size limit. This allows to skip files exeeding that limit during processing of workflow results. | 104857600
| AUTH_USERNAME | Basic auth username for this service, if required | None
| AUTH_PASSWORD | Basic auth password for this service, if required | None
| ROOT_PATH | Root path, useful if the app is behind a proxy. | None

## API Access

API documentation can be accessed at [https://localhost:8000/docs] (see [https://fastapi.tiangolo.com/reference/openapi/docs/](fastapi))
