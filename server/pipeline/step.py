import datetime
import threading
from dataclasses import dataclass
from typing import List, Self, Optional, Any

from .config import StepConfig
from .dto import StepDto
from .lock import pipelineMutex
from .pipeline import PipelineState
from .status import EventType

@dataclass
class Event:
    timestamp: datetime.datetime
    message: str
    type: EventType


class _Pipeline():
    """Dummy class"""
    def get_updated_state(self):
        """Trigger to check state of every step and derive step for pipeline"""
        pass

    @property
    def name(self) -> str:
        return ''

    @property
    def state(self) -> PipelineState:
        return PipelineState.RUNNING

    @property
    def id(self) -> int:
        return 0


class Step:
    counter_lock = threading.Lock()
    id_counter = 0

    def __init__(self, step_config: StepConfig, pipeline: _Pipeline, dependencies: List[Self]):
        self.id: Optional[int] = None
        self.state = PipelineState.OPEN
        self.step_config = step_config
        self.dependencies = dependencies
        self.pipeline = pipeline
        self.dependent_steps: List[Self] = []
        self.events: List[Event] = []
        self.result: Any = None
        for dependency in dependencies:
            dependency.dependent_steps.append(self)
        # TODO: write to DB
        with Step.counter_lock:
            self.id: Optional[int] = Step.id_counter
            Step.id_counter += 1

    def set_state(self, state: PipelineState):
        assert pipelineMutex.locked()
        # TODO: write to DB
        self.state = state
        self.pipeline.get_updated_state()

    async def run(self):
        self.events.append(Event(datetime.datetime.now(), "Pipeline step started", EventType.INFO))
        try:
            async for event, event_type in self.step_config.run():
                print(event)
                if event_type == EventType.RESULT:
                    self.result = event
                else:
                    self.events.append(Event(datetime.datetime.now(), event, event_type if event_type else EventType.INFO))
            # TODO: save event
        except Exception as e:
            self.events.append(Event(datetime.datetime.now(), f"Pipeline step failed with error: {e}", EventType.ERROR))
            raise e
        self.events.append(Event(datetime.datetime.now(), "Pipeline step ended", EventType.INFO))

    def name(self) -> str:
        return self.step_config.name()

    def display_name(self):
        return self.step_config.display_name()

    def serialize(self) -> StepDto:
        return StepDto(id=self.id, name=self.name(), state=self.state, displayName=self.display_name(), events=self.events, result=str(self.result) )
