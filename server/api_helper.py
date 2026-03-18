import re
from typing import List, Optional

import gridfs
from bson import ObjectId
from fastapi import FastAPI, HTTPException
from starlette.responses import Response

from .api.dto import PipelineDto, StepDto, PipelineConfigDto, StepConfigDto, PipelineCreation
from .config import PipelineConfig
from .pipeline import Step
from .server import PipelineServer
from .db import get_pipeline_db_client, get_raw_db_client


def add_common_api_calls(
    app: FastAPI, pipeline_server: PipelineServer, pipeline_config: List[PipelineConfig], api_base_url: str
) -> None:
    @app.get(api_base_url + "/hello-world/")
    async def hello_world():
        return {"message": "Hello World"}

    _config_enpoints(app, pipeline_config, api_base_url)
    _pipeline_endpoints(app, pipeline_server, pipeline_config, api_base_url)
    _step_endpoints(app, api_base_url)


def _config_enpoints(app: FastAPI, pipeline_config: List[PipelineConfig], api_base_url: str):
    available_pipelines = {pipeline.name: pipeline for pipeline in pipeline_config}

    @app.get(api_base_url + "/config/pipeline-types")
    async def get_pipeline_types() -> List[PipelineConfigDto]:
        return [PipelineConfigDto(pipeline) for pipeline in available_pipelines.values()]

    @app.get(api_base_url + "/config/pipeline-types/{pipeline_type}")
    async def get_pipeline_type_config(pipeline_type: str) -> List[StepConfigDto]:
        pipeline = available_pipelines.get(pipeline_type)
        if not pipeline:
            raise HTTPException(status_code=404, detail=f"Pipeline type '{pipeline_type}' not found")
        return [StepConfigDto(step) for step in pipeline.steps]


def _pipeline_endpoints(
    app: FastAPI, pipeline_server: PipelineServer, pipeline_config: List[PipelineConfig], api_base_url: str
):
    available_pipelines = {pipeline.name: pipeline for pipeline in pipeline_config}

    @app.get(api_base_url + "/pipelines")
    async def get_pipelines(
        types: Optional[List[str]] = None,
        state: Optional[List[str]] = None,
        name: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> List[PipelineDto]:
        pipeline_db = get_pipeline_db_client()
        query = {}
        if types:
            query["types"] = {"$in": types}
        if state:
            query["state"] = {"$in": state}
        if name:
            query["name"] = {"$regex": re.escape(name)}
        pipelines = pipeline_db.get_collection("pipelines").find(query, skip=offset, limit=limit)
        return [PipelineDto.from_entity(pipeline) for pipeline in pipelines]

    @app.get(api_base_url + "/pipelines/{pipeline_id}")
    async def get_pipeline(pipeline_id: str) -> Optional[PipelineDto]:
        pipeline_db = get_pipeline_db_client()
        pipeline = pipeline_db.get_collection("pipelines").find_one({"_id": ObjectId(pipeline_id)})

        print(pipeline_id, pipeline)
        if not pipeline:
            raise HTTPException(status_code=404, detail=f"Pipeline '{pipeline_id}' not found")
        return PipelineDto.from_entity(pipeline)

    @app.post(api_base_url + "/pipelines")
    async def create_pipeline(pipeline: PipelineCreation) -> PipelineDto:
        if pipeline.name not in available_pipelines or not (config := available_pipelines[pipeline.name]):
            raise HTTPException(status_code=404, detail="pipeline not found")
        return pipeline_server.add_pipeline(config, pipeline.config).serialize()


def _step_endpoints(app: FastAPI, api_base_url: str):
    @app.get(api_base_url + "/pipelines/{pipeline_id}/steps")
    async def get_pipeline_steps(pipeline_id: str) -> List[StepDto]:
        pipeline_db = get_pipeline_db_client()
        steps = pipeline_db.get_collection("steps").find({"pipeline": ObjectId(pipeline_id)})
        if steps is None:
            raise HTTPException(status_code=404, detail=f"Steps not found for pipeline '{pipeline_id}'")
        return [StepDto.from_entity(step) for step in steps]

    @app.get(api_base_url + "/pipelines/{pipeline_id}/steps/{step_id}/result")
    async def get_pipeline_steps_result(pipeline_id: str, step_id: str) -> Response:
        pipeline_db = get_pipeline_db_client()
        step = pipeline_db.get_collection("steps").find_one(
            {"_id": ObjectId(step_id), "pipeline": ObjectId(pipeline_id)}
        )
        if step is None:
            raise HTTPException(status_code=404, detail=f"Step {step_id} with pipeline {pipeline_id} not found")
        print(step, "result" not in step)
        if "result" not in step or "file" not in step["result"]:
            raise HTTPException(status_code=404, detail=f"Step {step_id} with pipeline {pipeline_id} has no result")
        file_id = step["result"]["file"]
        file_db = gridfs.GridFS(get_raw_db_client())
        file = file_db.get(ObjectId(file_id))
        if file is None:
            raise HTTPException(
                status_code=404, detail=f"Could not find file {file_id} for step {step_id} with pipeline {pipeline_id}"
            )
        response = Response(
            file.read(),
            media_type=Step.get_result_http_type(step["result"]["type"] if "type" in step["result"] else "text/plain"),
        )
        if file.filename:
            response.headers["Content-Disposition"] = f"inline; filename={file.filename}"
        return response
