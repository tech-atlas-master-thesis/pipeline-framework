from typing import List

from fastapi import FastAPI, HTTPException
from fastapi.params import Depends

from ..api.authentication import require_all_entitlements
from ..config import PipelineConfig
from ..dto.dto import PipelineConfigDto, StepConfigDto

AUTH_REQUIREMENTS_VIEW = require_all_entitlements("tech-atlas:read")


def config_enpoints(app: FastAPI, pipeline_config: List[PipelineConfig], api_base_url: str):
    available_pipelines = {pipeline.type: pipeline for pipeline in pipeline_config}

    @app.get(api_base_url + "/config/pipeline-types")
    async def get_pipeline_types(_=Depends(AUTH_REQUIREMENTS_VIEW)) -> List[PipelineConfigDto]:
        return [PipelineConfigDto(pipeline) for pipeline in available_pipelines.values()]

    @app.get(api_base_url + "/config/pipeline-types/{pipeline_type}")
    async def get_pipeline_type_config(pipeline_type: str, _=Depends(AUTH_REQUIREMENTS_VIEW)) -> List[StepConfigDto]:
        pipeline = available_pipelines.get(pipeline_type)
        if not pipeline:
            raise HTTPException(status_code=404, detail=f"Pipeline type '{pipeline_type}' not found")
        return [StepConfigDto(step) for step in pipeline.steps]
