from typing import List

from bson import ObjectId
from fastapi import FastAPI, HTTPException
from fastapi.params import Depends
from starlette.responses import Response

from .authentication import require_all_entitlements
from ..db import get_pipeline_db_client
from ..db.helper import get_file_from_db
from ..dto import StepDto
from ..pipeline import Step

AUTH_REQUIREMENTS_VIEW = require_all_entitlements("tech-atlas:read")


def step_endpoints(app: FastAPI, api_base_url: str):
    @app.get(api_base_url + "/pipelines/{pipeline_id}/steps")
    async def get_pipeline_steps(pipeline_id: str, _=Depends(AUTH_REQUIREMENTS_VIEW)) -> List[StepDto]:
        pipeline_db = get_pipeline_db_client()
        steps = pipeline_db.steps.find({"pipeline": ObjectId(pipeline_id)})
        if steps is None:
            raise HTTPException(status_code=404, detail=f"Steps not found for pipeline '{pipeline_id}'")
        return [StepDto.from_entity(step) async for step in steps]

    @app.get(api_base_url + "/pipelines/{pipeline_id}/steps/{step_id}/result")
    async def get_pipeline_steps_result(pipeline_id: str, step_id: str, _=Depends(AUTH_REQUIREMENTS_VIEW)) -> Response:
        pipeline_db = get_pipeline_db_client()
        step = await pipeline_db.steps.find_one({"_id": ObjectId(step_id), "pipeline": ObjectId(pipeline_id)})
        if step is None:
            raise HTTPException(status_code=404, detail=f"Step {step_id} with pipeline {pipeline_id} not found")
        if "result" not in step or "file" not in step["result"]:
            raise HTTPException(status_code=404, detail=f"Step {step_id} with pipeline {pipeline_id} has no result")
        file_id = step["result"]["file"]
        file = await get_file_from_db(ObjectId(file_id))
        if file is None:
            raise HTTPException(
                status_code=404, detail=f"Could not find file {file_id} for step {step_id} with pipeline {pipeline_id}"
            )
        response = Response(
            await file.read(),
            media_type=Step.get_result_http_type(step["result"]["type"] if "type" in step["result"] else "text/plain"),
        )
        if file.filename:
            response.headers["Content-Disposition"] = f"inline; filename={file.filename}"
        return response
