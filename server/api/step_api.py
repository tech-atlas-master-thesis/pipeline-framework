from typing import List

import gridfs
from bson import ObjectId
from fastapi import FastAPI, HTTPException
from starlette.responses import Response

from pipelineFramework.server.db import get_pipeline_db_client, get_raw_db_client
from pipelineFramework.server.dto.dto import (
    StepDto,
)
from pipelineFramework.server.pipeline import Step
from .authentication import verify_token


def step_endpoints(app: FastAPI, api_base_url: str):
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
