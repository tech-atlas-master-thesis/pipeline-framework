from typing import List, Optional

from fastapi import FastAPI, HTTPException
from starlette.responses import Response

from .api.dto import PipelineDto, StepDto, PipelineConfigDto, StepConfigDto, PipelineCreation
from .config import PipelineConfig
from .pipeline import Pipeline, Step
from .server import PipelineServer


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

    @app.get(api_base_url + "/config/pipeline-types/{pipeline_type}")
    async def get_pipeline_type_config(pipeline_type:str) -> List[StepConfigDto]:
        pipeline = available_pipelines.get(pipeline_type)
        if not pipeline:
            raise HTTPException(status_code=404, detail=f"Pipeline type '{pipeline_type}' not found")
        return [StepConfigDto(step) for step in pipeline.steps]

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
        return pipeline_server.add_pipeline(config, pipeline.config).serialize()
