from dataclasses import dataclass
from dataclasses import dataclass
from typing import List, Optional, Dict

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from starlette.responses import Response

from pipelineFramework import PipelineServer
from pipelineFramework.server.pipeline.config import PipelineConfig, StepConfig
from pipelineFramework.server.pipeline.dto import PipelineDto, StepDto
from pipelineFramework.server.pipeline.pipeline import Pipeline
from pipelineFramework.server.pipeline.step import Step


class PipelineCreation(BaseModel):
    name: str
    config: Optional[Dict[str, str]] = None


@dataclass
class StepConfigDto:
    name: str
    displayName: str

    def __init__(self, step: StepConfig):
        self.name = step.name()
        self.displayName = step.display_name()


@dataclass
class PipelineConfigDto:
    name: str
    displayName: str
    steps: List[StepConfigDto]

    def __init__(self, pipeline: PipelineConfig):
        self.name = pipeline.name
        self.displayName = pipeline.display_name
        self.steps = [StepConfigDto(step) for step in pipeline.steps]


def add_common_api_calls(app: FastAPI, pipeline_server: PipelineServer, pipeline_config: List[PipelineConfig], api_base_url: str) -> None:
    available_pipelines = {pipeline.name: pipeline for pipeline in pipeline_config}

    def get_pipeline_by_id(pipeline_id: int) -> Optional[Pipeline]:
        return [pipeline for pipeline in pipeline_server.pipelines if pipeline.id == pipeline_id][0]

    def get_step_by_id(pipeline_id: int, step_id: int) -> Optional[Step]:
        pipeline = get_pipeline_by_id(pipeline_id)
        return [step for step in pipeline.steps.values() if step.id == step_id][0]

    @app.get(api_base_url + "/hello-world/")
    async def hello_world():
        return {"message": "Hello World"}

    @app.get(api_base_url + "/config/pipeline-types")
    async def get_pipeline_types() -> List[PipelineConfigDto]:
        return [PipelineConfigDto(pipeline) for pipeline in available_pipelines.values()]

    @app.get(api_base_url + "/pipelines")
    async def get_pipelines() -> List[PipelineDto]:
        return [pipeline.serialize() for pipeline in pipeline_server.pipelines]

    @app.get(api_base_url + "/pipelines/{pipeline_id}")
    async def get_pipeline(pipeline_id: int) -> Optional[PipelineDto]:
        pipeline = get_pipeline_by_id(pipeline_id)
        return pipeline.serialize() if pipeline else None

    @app.get(api_base_url + "/pipelines/{pipeline_id}/steps")
    async def get_pipeline_steps(pipeline_id: int) -> List[StepDto]:
        pipeline = get_pipeline_by_id(pipeline_id)
        return [pipeline.serialize() for pipeline in pipeline.steps.values()] if pipeline else []

    @app.get(api_base_url + "/pipelines/{pipeline_id}/steps/{step_id}/result")
    async def get_pipeline_steps(pipeline_id: int, step_id: int) -> Response:
        step = get_step_by_id(pipeline_id, step_id)
        if step is None:
            raise HTTPException(status_code=404, detail="Step not found")
        response = step.get_result()
        if not response:
            raise HTTPException(status_code=404, detail="Step has no result")
        return response

    @app.post(api_base_url + "/pipelines")
    async def create_pipeline(pipeline: PipelineCreation) -> PipelineDto:
        if pipeline.name not in available_pipelines or not (config := available_pipelines[pipeline.name]):
            raise HTTPException(status_code=404, detail="pipeline not found")
        return pipeline_server.add_pipeline(config).serialize()
