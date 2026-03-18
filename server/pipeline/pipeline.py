import threading
from typing import Dict, Optional

from bson import ObjectId
from pygments.lexers import q
from pymongo.synchronous.collection import Collection
from pymongo.synchronous.database import Database

from .lock import pipelineMutex
from .step import Step
from ..api import PipelineDto
from ..config import PipelineConfig, UserConfig, PipelineState
from ..db import PipelineEntity


class Pipeline:
    def __init__(self, pipeline_config: PipelineConfig, user_config: Optional[UserConfig], pipeline_db: Database):
        self.pipeline_db: Collection = pipeline_db.get_collection("pipelines")
        self.config = pipeline_config
        self.steps: Dict[str, Step] = {}
        self.state = PipelineState.OPEN
        self.user_config = user_config
        previous_step: Optional[Step] = None
        parallelize = pipeline_config.parallelize
        print(PipelineEntity(None, self.config.name, self.config.name, self.state, self.user_config).to_json())
        self.id: ObjectId = self.pipeline_db.insert_one(
            {"type": self.config.name, "name": self.config.name, "state": self.state, "userConfig": self.user_config}
        ).inserted_id
        for step_config in pipeline_config.steps:
            user_step_config = user_config.get(step_config.name()) if user_config else None
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
            pass
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
    def name(self) -> str:
        return self.config.name

    def serialize(self) -> PipelineDto:
        return PipelineDto(
            id=str(self.id),
            name=self.name,
            state=self.state,
            displayName=self.config.display_name,
            description=self.config.description,
            userConfig=self.user_config,
        )
