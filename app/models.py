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

class WorkflowResponseModel(BaseModel):
    class Parameter(BaseModel):
        name: str
        value: str
    message: str = Field(None, description="Message from the workflow engine")
    workflow: dict = Field(None, description="Workflow definition")
    parameters: List[Parameter] = Field(None, description="Workflow parameters")

class WorkflowListResponseModel(BaseModel):
    workflow_name: str = Field(..., description="Internal name of the workflow")
    uid: str = Field(..., description="Internal UID of the workflow")
    name: str|None = Field(None, description="Name of the workflow")
    status: str = Field(..., description="Workflow status")
    createdAt: str = Field(..., description="Workflow creation time")
    startedAt: str = Field(..., description="Workflow started time")
    finishedAt: str|None = Field(..., description="Workflow finished time")
    submitterName: str|None = Field(None, description="Name of the user submitting the workflow")
    submitterOrcid: str|None = Field(None, description="Orcid of the user submitting the workflow")
