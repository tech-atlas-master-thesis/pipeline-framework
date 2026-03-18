import datetime
import json
from typing import List, Self, Optional, Any, Dict
from unittest import case

import gridfs
import pandas as pd
from bson import ObjectId
from gridfs import GridFS
from pymongo.synchronous.database import Database
from starlette.responses import Response

from .lock import pipelineMutex
from ..api.dto import StepDto, StepResultDto, StepResultType, Event
from ..config import StepConfig, UserStepConfig, PipelineState, EventType
from ..db import get_raw_db_client


class _Pipeline:
    """Dummy class"""

    def get_updated_state(self):
        """Trigger to check state of every step and derive step for pipeline"""
        pass

    @property
    def name(self) -> str:
        return ""

    @property
    def state(self) -> PipelineState:
        return PipelineState.RUNNING

    @property
    def id(self) -> int:
        return 0


class Step:
    @staticmethod
    def get_result_http_type(type: StepResultType) -> str:
        match type:
            case StepResultType.JSON:
                return "application/json"
            case StepResultType.CSV:
                return "text/csv"
        return "text/plain"

    def __init__(
        self,
        step_config: StepConfig,
        user_config: Optional[UserStepConfig],
        pipeline: _Pipeline,
        dependencies: List[Self],
        pipeline_db: Database,
    ):
        self.pipeline_db = pipeline_db.get_collection("steps")
        self.state = PipelineState.OPEN
        self.step_config = step_config
        self.dependencies = dependencies
        self.pipeline = pipeline
        self.dependent_steps: List[Self] = []
        self.events: List[Event] = []
        self.result: Optional[StepResultDto] = None
        self.user_config = user_config
        for dependency in dependencies:
            dependency.dependent_steps.append(self)
        self.id: ObjectId = self.pipeline_db.insert_one(
            {
                "pipeline": self.pipeline.id,
                "state": self.state,
                "name": self.step_config.name(),
                "events": self.events,
                "result": self.result,
            }
        ).inserted_id

    def set_state(self, state: PipelineState):
        assert pipelineMutex.locked()
        self.pipeline_db.update_one({"_id": self.id}, {"$set": {"state": state}})
        self.state = state
        self.pipeline.get_updated_state()

    async def run(self):
        self._add_event(Event(datetime.datetime.now(), "Pipeline step started", EventType.INFO))
        try:
            async for event, event_type in self.step_config.run(self.user_config):
                if event_type == EventType.RESULT:
                    self.result = self._save_result(event)
                else:
                    self._add_event(Event(datetime.datetime.now(), event, event_type if event_type else EventType.INFO))

        except Exception as e:
            self._add_event(Event(datetime.datetime.now(), f"Pipeline step failed with error: {e}", EventType.ERROR))
            raise e
        self._add_event(Event(datetime.datetime.now(), "Pipeline step ended", EventType.INFO))

    def _add_event(self, event: Event):
        self.events.append(event)
        self.pipeline_db.update_one(
            {"_id": self.id},
            {
                "$push": {
                    "events": {
                        "timestamp": event.timestamp,
                        "message": event.message,
                        "type": event.type,
                    }
                }
            },
        )

    def _save_result(self, result: Any):
        if result is None:
            return None

        preview = False
        result_type = StepResultType.STRING
        file_id = None
        data = None
        preview_data = None

        if isinstance(result, pd.DataFrame) or isinstance(result, pd.Series):
            preview_data = result.to_string(max_cols=5, max_rows=25)
            data = result.to_csv()
            preview = True
            result_type = StepResultType.CSV

        elif isinstance(result, dict):
            preview_data = json.dumps(dict.fromkeys(result, "..."))
            data = json.dumps(result)
            preview = True
            result_type = StepResultType.CSV

        if preview:
            file_id = self._save_file(data)

        self.pipeline_db.update_one(
            {"_id": self.id},
            {
                "$set": {
                    "result": {
                        "type": result_type,
                        "preview": preview,
                        "file": str(file_id),
                        "data": preview_data if file_id else str(result),
                    }
                }
            },
        )
        return StepResultDto(
            result_type, preview, str(file_id) if file_id else None, preview_data if file_id else str(result)
        )

    def _save_file(self, content: str) -> ObjectId:
        file_db = gridfs.GridFS(get_raw_db_client())
        return file_db.put(
            content,
            filename=f"{self.pipeline.name}-{self.pipeline.id}-{self.pipeline.name}-{self.name()}-{datetime.datetime.now().isoformat(timespec='seconds')}.csv",
            encoding="utf-8",
        )

    def name(self) -> str:
        return self.step_config.name()

    def display_name(self):
        return self.step_config.display_name()

    def serialize(self) -> StepDto:
        return StepDto(
            id=str(self.id),
            name=self.name(),
            state=self.state,
            displayName=self.display_name(),
            events=self.events,
            result=None,
            description=self.step_config.description(),
        )
