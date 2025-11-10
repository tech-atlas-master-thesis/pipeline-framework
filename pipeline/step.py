from abc import abstractmethod
from typing import Union, List, Self

from pipeline.pipeline import Pipeline
from pipeline.status import PipelineState

class Step:
    def __init__(self, dependencies: Union[None, List[Self]]):
        self.id: Union[int, None] = None
        self.state = PipelineState.OPEN
        self.dependencies = dependencies
        self._pipeline: Union[Pipeline, None] = None
        self._dependent_steps: List[Self] = []

    def _set_state(self, state: PipelineState):
        # TODO: write to DB
        self.state = state

    @abstractmethod
    async def execute(self):
        raise NotImplemented("Execution function not implemented")

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplemented("Name not implemented")
