from typing import Dict, Optional

from .config import PipelineConfig
from .lock import pipelineMutex
from .status import PipelineState
from .step import Step


class Pipeline:
    def __init__(self, pipeline_config: PipelineConfig):
        self.id: Optional[int] = None
        self.config = pipeline_config
        self.steps: Dict[str, Step] = {}
        self.state = PipelineState.OPEN
        for step in pipeline_config["steps"]:
            dependencies = [self.steps[step_name] for step_name in step.dependencies] if step.dependencies else []
            if any(dependency is None for dependency in dependencies):
                raise NameError(f"Step {step.name} is not (yet) defined")
            self.steps[step.name] = Step(step, self, dependencies)
        # TODO: write to DB
        self.id = 0

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
