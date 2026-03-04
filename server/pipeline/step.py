import datetime
import json
import threading
from dataclasses import dataclass
from typing import List, Self, Optional, Any

import pandas as pd
from starlette.responses import StreamingResponse, Response

from .config import StepConfig
from .dto import StepDto, StepResultDto, StepResultType
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
                if event_type == EventType.RESULT:
                    print("Result received")
                    self.result = event
                else:
                    print(event)
                    self.events.append(Event(datetime.datetime.now(), event, event_type if event_type else EventType.INFO))
            # TODO: save event
        except Exception as e:
            self.events.append(Event(datetime.datetime.now(), f"Pipeline step failed with error: {e}", EventType.ERROR))
            raise e
        self.events.append(Event(datetime.datetime.now(), "Pipeline step ended", EventType.INFO))

    def _get_result_dto(self) -> Optional[StepResultDto]:
        if self.result is None:
            return None

        print(type(self.result), isinstance(self.result, pd.DataFrame))
        if isinstance(self.result, pd.DataFrame) or isinstance(self.result, pd.Series):
            print(self.result.to_string(max_cols=2, max_rows=2))
            return StepResultDto(StepResultType.CSV, True, self.result.to_string(max_cols=5, max_rows=25))

        if isinstance(self.result, dict):
            return StepResultDto(StepResultType.JSON, True, json.dumps(self.result))

        return StepResultDto(StepResultType.STRING, False, str(self.result))

    def get_result(self) -> Optional[Response]:
        if self.result is None:
            return None

        if isinstance(self.result, pd.DataFrame) or isinstance(self.result, pd.Series):
            response = Response(self.result.to_csv(), media_type="text/csv")
            response.headers["Content-Disposition"] = f"inline; filename='pipeline_{self.pipeline.id}-{self.name()}-{self.events[-1].timestamp.isoformat()}.csv'"
            return response

        if isinstance(self.result, dict):
            return Response(json.dumps(self.result), media_type="application/json")

        return Response(str(self.result), media_type="text/plain")

    def name(self) -> str:
        return self.step_config.name()

    def display_name(self):
        return self.step_config.display_name()

    def serialize(self) -> StepDto:
        return StepDto(id=self.id, name=self.name(), state=self.state, displayName=self.display_name(), events=self.events, result=self._get_result_dto() )
