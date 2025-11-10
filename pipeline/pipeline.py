from abc import abstractproperty, abstractmethod
from asyncio import Lock
from typing import Union, List, Dict

from pipeline.status import PipelineState
from step import Step


pipelineMutex = Lock()


class Pipeline:
    def __init__(self, steps: List[Step]):
        # TODO: write to DB
        self.id = 0
        self._steps = self._prepare_steps(steps)
        self.state = PipelineState.OPEN

    def _prepare_steps(self, steps: List[Step]) -> List[Step]:
        steps: List[Step] = []
        for i, step in enumerate(steps):
            if i > 0 and step.dependencies is None:
                step.dependencies = [steps[i-1]]
            step._pipeline = self
            for dependent in step.dependencies:
                dependent._dependent_steps.append(step)
            # TODO: write to DB
            step.id = 0
            steps.append(step)
        return steps

    def _get_updated_state(self):
        old_state = self.state
        self.state = self._get_state()
        if old_state != self.state:
            # TODO: write to BD
            pass
        return self.state

    def _get_next_step(self)-> Union[Step,None]:
        if not pipelineMutex.locked():
            raise RuntimeError('Pipeline must be locked')
        for step in self.steps:
            if step.state == PipelineState.RUNNING:
                return None
            if step.state == PipelineState.OPEN:
                return step
        return None

    def _get_state(self):
        if not pipelineMutex.locked():
            raise RuntimeError('Pipeline must be locked')
        any_running = False
        any_open = True
        for step in self.steps:
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
    @abstractmethod
    def steps(self) -> list[Step]:
        raise NotImplementedError("Pipeline Steps not implemented")

    @property
    @abstractmethod
    def type(self) -> str:
        raise NotImplementedError("Pipeline Type not implemented")
