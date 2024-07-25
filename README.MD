# Argo-connector

A connector that pushes artifacts from an argo workflow engine to cordra.
Provides a REST api to notify the connector about finished argo workflows.
This then starts a background task that writes digital objects to cordra.

## Build

```
docker build . argo-connector .
```

## Deployment

Configuration via environment variables.
See the settings object in `app/main.py` to find out what settings are
expected. This is at least connection information for argo and cordra.

## API Access

API documentation can be accessed at [https://localhost:8000/docs] (see [https://fastapi.tiangolo.com/reference/openapi/docs/](fastapi))
