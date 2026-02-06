import asyncio
import threading
from asyncio import AbstractEventLoop
from typing import List, Tuple
from .pipeline.pipeline import Pipeline
from .pipeline.status import PipelineState
from .pipeline.step import Step
from .pipeline.config import PipelineConfig
from .pipeline.lock import pipelineMutex


class PipelineServer:
    thread_pool = asyncio.new_event_loop()

    def __init__(self):
        self.pipelines: List[Pipeline] = []

    def start_server(self):
        self.thread_pool.run_forever()

    def start_server_async(self) -> Tuple[threading.Thread, AbstractEventLoop]:
        thread =  threading.Thread(target=self.start_server)
        thread.start()
        return thread, self.thread_pool

    def add_pipeline(self, pipeline_config: PipelineConfig):
        pipeline = Pipeline(pipeline_config)
        self.pipelines.append(pipeline)
        with pipelineMutex:
            for _, pipeline_step in pipeline.steps.items():
                if all(dependency.state == PipelineState.FINISHED for dependency in pipeline_step.dependencies):
                    self.thread_pool.create_task(self._execute_step(pipeline_step))

    async def _execute_step(self, step: Step):
        pipeline = step.pipeline
        print("Start with step", step.name, pipeline, step)

        with pipelineMutex:
            step.set_state(PipelineState.RUNNING)

        try:
            print("Executing step", step.name)
            await step.run()
            print("Execution finished", step.name)
        except Exception as e:
            print(e)
            with pipelineMutex:
                step.set_state(PipelineState.ERROR)
                return

        with pipelineMutex:
            step.set_state(PipelineState.FINISHED)
            if pipeline.state != PipelineState.RUNNING:
                return

            for dependent in step.dependent_steps:
                if dependent.state != PipelineState.OPEN:
                    continue
                if all(dependency.state == PipelineState.FINISHED for dependency in dependent.dependencies):
                    self.thread_pool.create_task(self._execute_step(dependent))
