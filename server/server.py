import asyncio
from typing import List
from server.pipeline.pipeline import Pipeline
from server.pipeline.status import PipelineState
from server.pipeline.step import Step
from server.pipeline.config import PipelineConfig
from server.pipeline.lock import pipelineMutex


class PipelineServer:
    threadPool = asyncio.new_event_loop()

    def __init__(self):
        self.pipelines: List[Pipeline] = []
        self.threadPool.run_forever()

    def add_pipeline(self, pipeline_config: PipelineConfig):
        pipeline = Pipeline(pipeline_config)
        self.pipelines.append(pipeline)
        with pipelineMutex:
            for _, pipeline_step in pipeline.steps.items():
                if all(dependency.state == PipelineState.FINISHED for dependency in pipeline_step.dependencies):
                    self.threadPool.create_task(self._execute_step(pipeline_step))

    async def _execute_step(self, step: Step):
        pipeline = step.pipeline

        with pipelineMutex:
            step.set_state(PipelineState.RUNNING)

        try:
            await step.run()
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
                    self.threadPool.create_task(self._execute_step(dependent))
