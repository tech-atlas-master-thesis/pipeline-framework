import datetime
import re
from typing import Optional, List

from apscheduler import Scheduler
from apscheduler.triggers.cron import CronTrigger
from bson import ObjectId
from fastapi import HTTPException

from .pipeline_schedule import PipelineSchedule
from ..config import PipelineConfig
from ..db import get_pipeline_db_client
from ..dto import PaginatedListDto, PageDto, PipelineCreation, UserDto, AuditInfoDto
from ..dto.schedules import ScheduleCreation
from ..pipeline import Pipeline


class _PipelineServer:
    pipeline_configs: List[PipelineConfig]

    def add_pipeline(
        self, pipeline_configs: PipelineConfig, pipeline_creation: PipelineCreation, user: UserDto
    ) -> Pipeline:
        pass


class PipelineScheduler:
    def __init__(self, pipeline_server: _PipelineServer):
        self.db_client = get_pipeline_db_client().get_collection("schedules")
        self.pipeline_server = pipeline_server
        self.scheduler = Scheduler()
        self.current_jobs = {}
        self._restart_saved_schedules()
        self.scheduler.start_in_background()
        self.available_pipelines = {pipeline.type: pipeline for pipeline in self.pipeline_server.pipeline_configs}

    def _restart_saved_schedules(self):
        for schedule_entity in self.db_client.find():
            schedule = PipelineSchedule.from_entity(schedule_entity)
            if schedule.cron and schedule.active:
                self.scheduler.add_schedule(
                    self.run,
                    CronTrigger.from_crontab(schedule.cron),
                    id=schedule.id,
                    args=[schedule.id],
                )

    def add_schedule(self, schedule_creation: ScheduleCreation, user: UserDto) -> PipelineSchedule:
        schedule = PipelineSchedule(
            str(ObjectId()),
            schedule_creation.to_pipeline(),
            schedule_creation.active,
            schedule_creation.cron,
            AuditInfoDto(user, datetime.datetime.now(datetime.UTC)),
        )
        self.db_client.insert_one(schedule.to_entity())
        if schedule.cron and schedule.active:
            self.scheduler.add_schedule(
                self.run,
                CronTrigger.from_crontab(schedule.cron),
                id=schedule.id,
                args=[schedule.id],
            )
        return schedule

    def get_schedules(
        self,
        type: Optional[List[str]] = None,
        active: Optional[List[str]] = None,
        name: Optional[str] = None,
        sort: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> PaginatedListDto[PipelineSchedule]:
        query = {}
        if type:
            query["pipeline.type"] = {"$in": type}
        if active is not None:
            query["active"] = {"$is": active}
        if name:
            query["pipeline.name"] = {"$regex": re.escape(name)}
        if sort:
            single_sorts = (single_sort.split(":") for single_sort in sort.split(";"))
            sort_query = {field: int(order) for field, order in single_sorts}
        else:
            sort_query = {"_id": -1}
        schedules = self.db_client.find(query).sort(sort_query).skip(offset).limit(limit)
        total_records = self.db_client.count_documents(query)
        return PaginatedListDto(
            [PipelineSchedule.from_entity(schedule) for schedule in schedules], PageDto(offset, limit, total_records)
        )

    def get_schedule(self, schedule_id: str) -> PipelineSchedule:
        schedule = self.db_client.find_one({"_id": ObjectId(schedule_id)})
        if not schedule:
            raise HTTPException(status_code=404, detail=f"Schedule with id {schedule_id} not found")
        return PipelineSchedule.from_entity(schedule)

    def update_schedule(self, schedule_id: str, schedule_update: ScheduleCreation, user: UserDto) -> PipelineSchedule:
        previous_schedule = self.get_schedule(schedule_id)
        if previous_schedule.cron and previous_schedule.active:
            self.scheduler.remove_schedule(previous_schedule.id)
        schedule = self._update_schedule(previous_schedule, schedule_update)
        schedule.modified = AuditInfoDto(user, datetime.datetime.now(datetime.UTC))
        self.db_client.replace_one({"_id": ObjectId(schedule_id)}, schedule.to_entity())
        if schedule.cron and schedule.active:
            self.scheduler.add_schedule(
                self.run, CronTrigger.from_crontab(schedule.cron), id=schedule.id, args=[schedule_id]
            )
        return schedule

    def run(self, schedule_id: str, user: Optional[UserDto] = None) -> Pipeline:
        schedule_entity = self.db_client.find_one({"_id": ObjectId(schedule_id)})
        if not schedule_entity:
            raise FileNotFoundError(f"Schedule with id {schedule_id} not found")
        schedule = PipelineSchedule.from_entity(schedule_entity)
        pipeline_config = self.available_pipelines.get(schedule.pipeline.type)
        if not pipeline_config:
            raise FileNotFoundError(f"Schedule with type {schedule.pipeline.type} not found")
        pipeline = self.pipeline_server.add_pipeline(
            pipeline_config,
            schedule.pipeline,
            user if user is not None else schedule.created.by,
        )
        self.db_client.update_one(
            {"_id": ObjectId(schedule_id)},
            {"$set": {"last_pipeline": pipeline.id, "last_execution": pipeline.created.at}},
        )
        return pipeline

    def _update_schedule(self, schedule: PipelineSchedule, schedule_creation: ScheduleCreation) -> PipelineSchedule:
        return PipelineSchedule(
            schedule.id,
            schedule_creation.pipeline,
            schedule_creation.active,
            schedule_creation.cron,
            schedule.created,
            schedule.modified,
            schedule.lastExecution,
            schedule.lastPipeline,
        )
