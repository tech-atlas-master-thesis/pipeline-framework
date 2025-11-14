from abc import abstractmethod
from typing import Union, List, Self, Coroutine

from server.pipeline.lock import pipelineMutex
from server.pipeline.pipeline import Pipeline
from server.pipeline.pipeline import PipelineState

class Step:
    @classmethod
    def step_factory(cls, name: str, run: Coroutine[None, None, None]):
        class FactoryStep(cls):
            def name(self) -> str:
                return name
            async def run(self):
                return await run()
        return FactoryStep

    def __init__(self, dependencies: Union[None, List[Self]] = None):
        self.id: Union[int, None] = None
        self.state = PipelineState.OPEN
        self.dependencies = dependencies
        self.pipeline: Union[Pipeline, None] = None
        self.dependent_steps: List[Self] = []

    def set_state(self, state: PipelineState):
        assert pipelineMutex.locked()
        # TODO: write to DB
        self.state = state

    @abstractmethod
    async def run(self):
        raise NotImplemented("Execution function not implemented")

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplemented("Name not implemented")
