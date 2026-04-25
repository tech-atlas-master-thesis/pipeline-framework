from typing import List

from fastapi import FastAPI, HTTPException

from pipelineFramework.server.config import PipelineConfig
from pipelineFramework.server.dto.dto import PipelineConfigDto, StepConfigDto


def config_enpoints(app: FastAPI, pipeline_config: List[PipelineConfig], api_base_url: str):
    available_pipelines = {pipeline.type: pipeline for pipeline in pipeline_config}

    @app.get(api_base_url + "/config/pipeline-types")
    async def get_pipeline_types() -> List[PipelineConfigDto]:
        return [PipelineConfigDto(pipeline) for pipeline in available_pipelines.values()]

    @app.get(api_base_url + "/config/pipeline-types/{pipeline_type}")
    async def get_pipeline_type_config(pipeline_type: str) -> List[StepConfigDto]:
        pipeline = available_pipelines.get(pipeline_type)
        if not pipeline:
            raise HTTPException(status_code=404, detail=f"Pipeline type '{pipeline_type}' not found")
        return [StepConfigDto(step) for step in pipeline.steps]
