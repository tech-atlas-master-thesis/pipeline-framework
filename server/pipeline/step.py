from typing import Union, List, Self

from .config import StepConfig
from .lock import pipelineMutex
from .pipeline import PipelineState


class _Pipeline():
    """Dummy class"""
    def get_updated_state(self):
        """Trigger to check state of every step and derive step for pipeline"""
        pass


class Step:
    def __init__(self, step_config: StepConfig, pipeline: _Pipeline, dependencies: List[Self]):
        self.id: Union[int, None] = None
        self.state = PipelineState.OPEN
        self.step_config = step_config
        self.dependencies = dependencies
        self.pipeline = pipeline
        self.dependent_steps: List[Self] = []
        for dependency in dependencies:
            dependency.dependent_steps.append(self)
        print(step_config, dependencies)

    def set_state(self, state: PipelineState):
        assert pipelineMutex.locked()
        # TODO: write to DB
        self.state = state
        self.pipeline.get_updated_state()

    async def run(self):
        cor = self.step_config.run()
        try:
            while True:
                event = cor.send(None)
                # TODO: save event
        except StopAsyncIteration as e:
            return e.value


    @property
    def name(self) -> str:
        return self.step_config.name

    @property
    def display_name(self):
        return self.step_config.display_name
