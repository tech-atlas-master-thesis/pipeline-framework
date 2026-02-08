import asyncio
import logging
from typing import List

from .pipeline.config import PipelineConfig
from .pipeline.lock import pipelineMutex
from .pipeline.pipeline import Pipeline
from .pipeline.status import PipelineState
from .pipeline.step import Step

logger = logging.getLogger(__name__)


class PipelineServer:
    running_tasks: List[asyncio.Task] = []

    def __init__(self):
        self.pipelines: List[Pipeline] = []

    def add_pipeline(self, pipeline_config: PipelineConfig):
        pipeline = Pipeline(pipeline_config)
        self.pipelines.append(pipeline)
        logger.info(f"Added pipeline '{pipeline.name}'")
        with pipelineMutex:
            for _, pipeline_step in pipeline.steps.items():
                if all(dependency.state == PipelineState.FINISHED for dependency in pipeline_step.dependencies):
                    logger.debug(f"Added pipeline step, '{pipeline_step.name()}' ({pipeline_step.id}) from pipeline '{pipeline.name}' ({pipeline.id})")
                    self.running_tasks.append(asyncio.create_task(self._execute_step(pipeline_step)))

    async def _execute_step(self, step: Step):
        logger.info(f"Executing step '{step.name()}'")
        pipeline = step.pipeline

        with pipelineMutex:
            step.set_state(PipelineState.RUNNING)
        logger.debug(f"Execute step, '{step.name()}' ({step.id}) from pipeline ''{pipeline.name}' ({pipeline.id})")

        try:
            await step.run()
            logger.debug(f"Finished executing step, '{step.name()}' ({step.id}) from pipeline '{pipeline.name}' ({pipeline.id})")
        except Exception as e:
            with pipelineMutex:
                logger.warning(f"Step '{step.name()}' ({step.id}) from pipeline '{pipeline.name}' ({pipeline.id}) ran into an error ({e})")
                step.set_state(PipelineState.ERROR)
                return

        with pipelineMutex:
            step.set_state(PipelineState.FINISHED)
            if pipeline.state == PipelineState.FINISHED:
                logger.info(f"Pipeline '{pipeline.name}' ({pipeline.id}) finished with state {pipeline.state}")
                return

            for dependent in step.dependent_steps:
                if dependent.state != PipelineState.OPEN:
                    continue
                if all(dependency.state == PipelineState.FINISHED for dependency in dependent.dependencies):
                    logger.debug(f"Added pipeline step, '{dependent.name()}' ({dependent.id}) from pipeline '{pipeline.name}' ({pipeline.id})")
                    asyncio.create_task(self._execute_step(dependent))
