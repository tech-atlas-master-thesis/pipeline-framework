import re
from typing import List, Optional, Annotated

from bson import ObjectId
from fastapi import FastAPI, Query, HTTPException
from fastapi.params import Depends

from pipelineFramework.server.config import PipelineConfig
from pipelineFramework.server.db import get_pipeline_db_client
from pipelineFramework.server.dto.dto import PipelineDto, PipelineCreation, PaginatedListDto, PageDto
from pipelineFramework.server.server import PipelineServer
from .authentication import require_all_entitlements

AUTH_REQUIREMENTS_VIEW = require_all_entitlements("tech-atlas:read")
AUTH_REQUIREMENTS_EDIT = require_all_entitlements("tech-atlas:write")


def pipeline_endpoints(
    app: FastAPI, pipeline_server: PipelineServer, pipeline_config: List[PipelineConfig], api_base_url: str
):
    available_pipelines = {pipeline.type: pipeline for pipeline in pipeline_config}

    @app.get(api_base_url + "/pipelines")
    async def get_pipelines(
        type: Annotated[Optional[List[str]], Query()] = None,
        state: Annotated[Optional[List[str]], Query()] = None,
        name: Optional[str] = None,
        sort: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
        _=Depends(AUTH_REQUIREMENTS_VIEW),
    ) -> PaginatedListDto[PipelineDto]:
        pipeline_db = get_pipeline_db_client()
        query = {}
        if type:
            query["type"] = {"$in": type}
        if state:
            query["state"] = {"$in": state}
        if name:
            query["name"] = {"$regex": re.escape(name)}
        if sort:
            single_sorts = (single_sort.split(":") for single_sort in sort.split(";"))
            sort_query = {field: int(order) for field, order in single_sorts}
        else:
            sort_query = {"_id": -1}
        pipelines = pipeline_db.get_collection("pipelines").find(query).sort(sort_query).skip(offset).limit(limit)
        total_records = pipeline_db.get_collection("pipelines").count_documents(query)
        return PaginatedListDto(
            [PipelineDto.from_entity(pipeline) for pipeline in pipelines], PageDto(offset, limit, total_records)
        )

    @app.get(api_base_url + "/pipelines/{pipeline_id}")
    async def get_pipeline(pipeline_id: str, _=Depends(AUTH_REQUIREMENTS_VIEW)) -> Optional[PipelineDto]:
        pipeline_db = get_pipeline_db_client()
        pipeline = pipeline_db.get_collection("pipelines").find_one({"_id": ObjectId(pipeline_id)})

        if not pipeline:
            raise HTTPException(status_code=404, detail=f"Pipeline '{pipeline_id}' not found")
        return PipelineDto.from_entity(pipeline)

    @app.post(api_base_url + "/pipelines")
    async def create_pipeline(pipeline: PipelineCreation, user=Depends(AUTH_REQUIREMENTS_EDIT)) -> PipelineDto:
        if pipeline.type not in available_pipelines or not (config := available_pipelines[pipeline.type]):
            raise HTTPException(status_code=404, detail="pipeline not found")
        return pipeline_server.add_pipeline(config, pipeline, user).serialize()
