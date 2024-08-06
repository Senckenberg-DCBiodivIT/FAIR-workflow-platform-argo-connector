from typing import List

from pydantic import BaseModel, Field


class HealthModel(BaseModel):
    cordra_connection: str = Field("true", description="State of cordra connection")
    argo_connection: str = Field("true", description="State of argo connection")

class NotificationResponseModel(BaseModel):
    class Artifact(BaseModel):
        node_id: str
        path: str

    status: str = Field("accepted", description="Accepted or Rejected")
    workflow_status: str = Field("Succeeded", description="Current state of the workflow")
    workflow_name: str = Field(None, description="Name of the workflow")
    workflow_namespace: str = Field(None, description="Namespace of the workflow")
    artifacts: List[Artifact]