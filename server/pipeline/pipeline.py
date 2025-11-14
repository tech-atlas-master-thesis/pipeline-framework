from abc import abstractmethod
from asyncio import Lock
from typing import Union, List, Tuple, Mapping

from server.pipeline.status import PipelineState
from step import Step


pipelineMutex = Lock()


class Pipeline:
    @classmethod
    def pipeline_with_consecutive_steps(cls, name: str, steps: List[Step]):
        prepared_steps: List[Step] = []
        for i, step in enumerate(steps):
            if i > 0 and step.dependencies is None:
                step.dependencies = [steps[i-1]]
            prepared_steps.append(step)

        return cls(name, prepared_steps)

    def __init__(self, name: str, steps: List[Step]):
        # TODO: write to DB with steps
        self.id = 0
        self.name = name
        self.steps = steps
        self.state = PipelineState.OPEN
        for step in steps:
            step.pipeline = self
        for step in steps:
            for dependency in step.dependencies:
                if dependency.dependent_steps is None:
                    dependency.dependent_steps = []
                dependency.dependent_steps.append(step)

    def get_updated_state(self):
        assert pipelineMutex.locked()
        old_state = self.state
        self.state = self._get_state()
        if old_state != self.state:
            # TODO: write to BD
            pass
        return self.state

    def _get_state(self):
        assert pipelineMutex.locked()
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
