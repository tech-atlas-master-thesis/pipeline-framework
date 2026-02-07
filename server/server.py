import asyncio
import logging
import threading
from typing import List, Tuple, Optional
from .pipeline.pipeline import Pipeline
from .pipeline.status import PipelineState
from .pipeline.step import Step
from .pipeline.config import PipelineConfig
from .pipeline.lock import pipelineMutex


logger = logging.getLogger(__name__)


class PipelineServer:
    thread_pool = asyncio.new_event_loop()
    active_thread: Optional[threading.Thread] = None

    def __init__(self):
        self.pipelines: List[Pipeline] = []

    def start_server(self):
        logger.info("Starting pipeline server")
        self.thread_pool.run_forever()
        logger.info("Pipeline server stopped")

    def stop_server(self):
        logger.info("Stopping pipeline server")
        self.thread_pool.stop()

    def start_server_async(self):
        if self.active_thread is not None:
            logger.error("Pipeline server already running")
        self.active_thread = threading.Thread(target=self.start_server)
        self.active_thread.start()

    def stop_server_async(self) -> threading.Thread:
        self.stop_server()
        thread = self.active_thread
        self.active_thread = None
        return thread

    def add_pipeline(self, pipeline_config: PipelineConfig):
        pipeline = Pipeline(pipeline_config)
        self.pipelines.append(pipeline)
        logger.info(f"Added pipeline {pipeline.name}")
        with pipelineMutex:
            for _, pipeline_step in pipeline.steps.items():
                if all(dependency.state == PipelineState.FINISHED for dependency in pipeline_step.dependencies):
                    logger.debug(f"Added pipeline step, {pipeline_step.name} ({pipeline_step.id}) from pipeline {pipeline.name} ({pipeline.id})")
                    self.thread_pool.create_task(self._execute_step(pipeline_step))

    async def _execute_step(self, step: Step):
        pipeline = step.pipeline

        with pipelineMutex:
            step.set_state(PipelineState.RUNNING)
        logger.debug(f"Execute step, {step.name} ({step.id}) from pipeline {pipeline.name} ({pipeline.id})")

        try:
            await step.run()
            logger.debug(f"Finished executing step, {step.name} ({step.id}) from pipeline {pipeline.name} ({pipeline.id})")
        except Exception as e:
            with pipelineMutex:
                logger.warning(f"Step {step.name} ({step.id}) from pipeline {pipeline.name} ({pipeline.id}) ran into an error ({e})")
                step.set_state(PipelineState.ERROR)
                return

        with pipelineMutex:
            step.set_state(PipelineState.FINISHED)
            if pipeline.state != PipelineState.RUNNING:
                logger.info(f"Pipeline {pipeline.name} ({pipeline.id}) finished with state {pipeline.state}")
                return

            for dependent in step.dependent_steps:
                if dependent.state != PipelineState.OPEN:
                    continue
                if all(dependency.state == PipelineState.FINISHED for dependency in dependent.dependencies):
                    logger.debug(f"Added pipeline step, {step.name} ({step.id}) from pipeline {pipeline.name} ({pipeline.id})")
                    self.thread_pool.create_task(self._execute_step(dependent))
