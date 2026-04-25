import datetime
from typing import Dict, Optional

from bson import ObjectId
from pygments.lexers import q
from pymongo.synchronous.collection import Collection
from pymongo.synchronous.database import Database

from .lock import pipelineMutex
from .step import Step
from ..config import PipelineConfig, PipelineState
from ..dto import PipelineDto, PipelineCreation
from ..dto.dto import AuditInfoDto, UserDto


class Pipeline:
    def __init__(
        self,
        pipeline_config: PipelineConfig,
        pipeline_creation: PipelineCreation,
        pipeline_db: Database,
        user: UserDto,
    ):
        self.pipeline_db: Collection = pipeline_db.get_collection("pipelines")
        self.config = pipeline_config
        self.name = pipeline_creation.name
        self.description = pipeline_creation.description
        self.steps: Dict[str, Step] = {}
        self.state = PipelineState.OPEN
        self.user_config = pipeline_creation.config
        self.created = AuditInfoDto(user, datetime.datetime.now(datetime.UTC))
        self.results = {}
        previous_step: Optional[Step] = None
        parallelize = pipeline_config.parallelize
        self.id: ObjectId = self.pipeline_db.insert_one(
            {
                "type": self.config.type,
                "name": self.name,
                "description": self.description,
                "state": self.state,
                "userConfig": self.user_config,
                "created": self.created.serialize(),
            }
        ).inserted_id
        for step_config in pipeline_config.steps:
            user_step_config = self.user_config.get(step_config.name()) if self.user_config else None
            if parallelize:
                dependencies = (
                    [self.steps[step_name] for step_name in step_config.dependencies()]
                    if step_config.dependencies()
                    else []
                )
                if any(dependency is None for dependency in dependencies):
                    raise NameError(f"Step {step_config.name} is not (yet) defined")
            else:
                dependencies = [previous_step] if previous_step is not None else []
            step = Step(step_config, user_step_config, self, dependencies, pipeline_db)
            self.steps[step_config.name()] = step
            if not parallelize:
                previous_step = step

    def get_updated_state(self):
        assert pipelineMutex.locked()
        old_state = self.state
        self.state = self._get_state()
        if old_state != self.state:
            self.pipeline_db.update_one({"_id": self.id}, {"$set": {"state": self.state}})
        return self.state

    def _get_state(self):
        assert pipelineMutex.locked()
        any_running = False
        any_open = False
        for step in self.steps.values():
            if step.state == PipelineState.ERROR:
                return PipelineState.ERROR
            if step.state == PipelineState.RUNNING:
                any_running = True
            if step.state == PipelineState.OPEN:
                any_open = True
        if any_running:
            return PipelineState.RUNNING
        if any_open:
            return PipelineState.OPEN
        return PipelineState.FINISHED

    @property
    def type(self) -> str:
        return self.config.type

    def serialize(self) -> PipelineDto:
        return PipelineDto(
            id=str(self.id),
            type=self.type,
            name=self.name,
            description=self.description,
            state=self.state,
            userConfig=self.user_config,
            created=self.created,
        )
