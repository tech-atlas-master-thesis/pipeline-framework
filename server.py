import asyncio
from typing import List
from pipeline.pipeline import Pipeline, pipelineMutex
from pipeline.status import PipelineState
from pipeline.step import Step


class PipelineServer:
    threadPool = asyncio.get_event_loop()

    def __init__(self):
        self.pipelines: List[Pipeline] = []
        self.threadPool.run_forever()

    def add_pipeline(self, pipeline: Pipeline):
        # TODO: write to database
        self.pipelines.append(pipeline)
        for pipeline_step in pipeline.steps:
            if all(dependency.state == PipelineState.FINISHED for dependency in pipeline_step.dependencies):
                self.threadPool.create_task(self._execute_step(pipeline_step))

    async def _execute_step(self, step: Step):
        pipeline = step._pipeline

        try:
            await step.execute()
        except Exception as e:
            print(e)
            with pipelineMutex:
                step._set_state(PipelineState.ERROR)
                pipeline._get_updated_state()
                return

        with pipelineMutex:
            step._set_state(PipelineState.FINISHED)
            if pipeline._get_updated_state() != PipelineState.RUNNING:
                return

            for dependent in step._dependent_steps:
                if dependent.state != PipelineState.OPEN:
                    continue
                if all(dependency.state == PipelineState.FINISHED for dependency in dependent.dependencies):
                    dependent._set_state(PipelineState.FINISHED)
                    self.threadPool.create_task(self._execute_step(dependent))


