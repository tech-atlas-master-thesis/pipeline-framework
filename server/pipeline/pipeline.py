import threading
from typing import Dict, Optional

from .config import PipelineConfig
from .lock import pipelineMutex
from .status import PipelineState
from .step import Step


class Pipeline:
    counter_lock = threading.Lock()
    id_counter = 0

    def __init__(self, pipeline_config: PipelineConfig):
        self.config = pipeline_config
        self.steps: Dict[str, Step] = {}
        self.state = PipelineState.OPEN
        previous_step: Optional[Step] = None
        parallelize = "parallelize"  in pipeline_config and pipeline_config["parallelize"]
        for step_config in pipeline_config["steps"]:
            if parallelize:
                dependencies = [self.steps[step_name] for step_name in step_config.dependencies()] if step_config.dependencies() else []
                if any(dependency is None for dependency in dependencies):
                    raise NameError(f"Step {step_config.name} is not (yet) defined")
            else:
                dependencies = [previous_step] if previous_step is not None else []
            step = Step(step_config, self, dependencies)
            self.steps[step_config.name()] = step
            if not parallelize:
                previous_step = step
        # TODO: write to DB
        with Pipeline.counter_lock:
            self.id: Optional[int] = Pipeline.id_counter
            Pipeline.id_counter += 1

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
        return self.config['name']
