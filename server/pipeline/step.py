import threading
from typing import List, Self, Optional

from .config import StepConfig
from .lock import pipelineMutex
from .pipeline import PipelineState


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
        async for event in self.step_config.run():
            print(event)
            # TODO: save event

    def name(self) -> str:
        return self.step_config.name()

    def display_name(self):
        return self.step_config.display_name()
