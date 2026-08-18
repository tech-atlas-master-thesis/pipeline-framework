import asyncio
import logging
import traceback
from typing import List, Optional

from .config import PipelineConfig, PipelineState
from .configuration import Configuration
from .db import get_pipeline_db_client
from .dto import PipelineCreation
from .dto.dto import UserDto
from .pipeline import Pipeline, Step
from .pipeline.lock import pipelineMutex
from .schedules.pipeline_scheduler import PipelineScheduler

logger = logging.getLogger(__name__)


class PipelineServer:
    running_tasks: List[asyncio.Task] = []

    def __init__(self, pipeline_configs: List[PipelineConfig], config_definitions: List[Configuration]):
        self.event_loop: Optional[asyncio.AbstractEventLoop] = None
        self.pipelines: List[Pipeline] = []
        self.pipeline_db_client = get_pipeline_db_client()
        self.pipeline_configs = pipeline_configs
        self.config_definitions = config_definitions
        self.scheduler = PipelineScheduler(self)

    async def bind_event_loop(self, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        self.event_loop = loop or asyncio.get_running_loop()
        await self.scheduler.initialize()

    async def add_pipeline(
        self,
        pipeline_config: PipelineConfig,
        pipeline_creation: PipelineCreation,
        user: UserDto,
        schedule_id: Optional[str] = None,
    ) -> Pipeline:
        pipeline = Pipeline(pipeline_config, pipeline_creation, self.pipeline_db_client, user, schedule_id=schedule_id)
        await pipeline.initialize()
        self.pipelines.append(pipeline)
        logger.info(f"Added pipeline '{pipeline.name}'")
        with pipelineMutex:
            for _, pipeline_step in pipeline.steps.items():
                if all(dependency.state == PipelineState.FINISHED for dependency in pipeline_step.dependencies):
                    logger.debug(
                        f"Added pipeline step, '{pipeline_step.name()}' ({pipeline_step.id}) from pipeline '{pipeline.name}' ({pipeline.id})"
                    )
                    self.running_tasks.append(self.event_loop.create_task(self._execute_step(pipeline_step), eager_start=False))
        return pipeline

    async def _execute_step(self, step: Step):
        logger.info(f"Executing step '{step.name()}'")
        pipeline = step.pipeline

        with pipelineMutex:
            await step.set_state(PipelineState.RUNNING)
        logger.debug(f"Execute step, '{step.name()}' ({step.id}) from pipeline ''{pipeline.name}' ({pipeline.id})")

        try:
            await step.run()
            logger.debug(
                f"Finished executing step, '{step.name()}' ({step.id}) from pipeline '{pipeline.name}' ({pipeline.id})"
            )
        except Exception as e:
            with pipelineMutex:
                logger.debug(traceback.format_exc())
                logger.warning(
                    f"Step '{step.name()}' ({step.id}) from pipeline '{pipeline.name}' ({pipeline.id}) ran into an error ({e})"
                )
                await step.set_state(PipelineState.ERROR)
                return

        with pipelineMutex:
            await step.set_state(PipelineState.FINISHED)
            if pipeline.state == PipelineState.FINISHED:
                logger.info(f"Pipeline '{pipeline.name}' ({pipeline.id}) finished with state {pipeline.state}")
                return

            for dependent in step.dependent_steps:
                if dependent.state != PipelineState.OPEN:
                    continue
                if all(dependency.state == PipelineState.FINISHED for dependency in dependent.dependencies):
                    logger.debug(
                        f"Added pipeline step, '{dependent.name()}' ({dependent.id}) from pipeline '{pipeline.name}' ({pipeline.id})"
                    )
                    self.event_loop.create_task(self._execute_step(dependent))
