from typing import List, Optional, Annotated

from fastapi import FastAPI, Query, HTTPException
from fastapi.params import Depends

from .authentication import require_all_entitlements
from ..dto import (
    PaginatedListDto,
)
from ..dto.schedules import ScheduleCreation
from ..schedules.pipeline_schedule import PipelineSchedule
from ..server import PipelineServer

AUTH_REQUIREMENTS_VIEW = require_all_entitlements("tech-atlas:read")
AUTH_REQUIREMENTS_EDIT = require_all_entitlements("tech-atlas:write")


def schedule_endpoints(app: FastAPI, pipeline_server: PipelineServer, api_base_url: str):
    available_pipelines = {pipeline.type: pipeline for pipeline in pipeline_server.pipeline_configs}

    @app.get(api_base_url + "/schedules")
    async def get_schedules(
        type: Annotated[Optional[List[str]], Query()] = None,
        active: Annotated[Optional[bool], Query()] = None,
        name: Optional[str] = None,
        sort: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
        _=Depends(AUTH_REQUIREMENTS_VIEW),
    ) -> PaginatedListDto[PipelineSchedule]:
        return await pipeline_server.scheduler.get_schedules(type, active, name, sort, limit, offset)

    @app.post(api_base_url + "/schedules")
    async def create_pipeline(schedule: ScheduleCreation, user=Depends(AUTH_REQUIREMENTS_EDIT)) -> PipelineSchedule:
        if schedule.pipeline.type not in available_pipelines or not available_pipelines[schedule.pipeline.type]:
            raise HTTPException(status_code=404, detail=f"Pipeline type {schedule.pipeline.type} not found")
        return (await pipeline_server.scheduler.add_schedule(schedule, user)).serialize()

    @app.get(api_base_url + "/schedules/{schedule_id}")
    async def get_pipeline(schedule_id: str, _=Depends(AUTH_REQUIREMENTS_VIEW)) -> Optional[PipelineSchedule]:
        return (await pipeline_server.scheduler.get_schedule(schedule_id)).serialize()

    @app.put(api_base_url + "/schedules/{schedule_id}")
    async def update_pipeline(
        schedule_id: str, schedule: ScheduleCreation, user=Depends(AUTH_REQUIREMENTS_EDIT)
    ) -> PipelineSchedule:
        return (await pipeline_server.scheduler.update_schedule(schedule_id, schedule, user)).serialize()

    @app.post(api_base_url + "/schedules/{schedule_id}/trigger", status_code=204)
    async def trigger_pipeline(schedule_id: str, user=Depends(AUTH_REQUIREMENTS_EDIT)):
        await pipeline_server.scheduler.run(schedule_id, user)
