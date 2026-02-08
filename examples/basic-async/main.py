import asyncio
import logging

from server import PipelineServer, StepConfig, PipelineConfig, PipelineState
from tests.test_helper import execute_test

logging.basicConfig(level=logging.DEBUG)


class BasicFirstStep(StepConfig):
    def display_name(self):
        return "Basic First Step"
    def name(self):
        return "basic-first-step"
    async def run(self):
        yield 'First Step Start'
        await asyncio.sleep(0.1)
        print("Basic First Step")
        yield 'First Step End'

class BasicSecondStep(StepConfig):
    @property
    def display_name(self):
        return "Basic Second Step"
    def name(self) -> str:
        return "basic-second-step"
    async def run(self):
        yield 'Second Step Start'
        await asyncio.sleep(0.1)
        print("Basic Second Step")
        yield 'Second Step End'

async def check_running(pipeline_server: PipelineServer, timeout: int):
    async with asyncio.timeout(timeout):
        while True:
            await asyncio.sleep(1)
            if all(pipeline.state == PipelineState.FINISHED for pipeline in pipeline_server.pipelines):
                return True


async def start_test(pipeline_server: PipelineServer):
    pipeline: PipelineConfig = {
        'name': 'basic-first-step',
        'display_name': 'Basic First Step',
        'steps': [BasicFirstStep(), BasicSecondStep()],
    }

    pipeline_server.add_pipeline(pipeline)

def main():
    execute_test(start_test)


if __name__ == "__main__":
    main()
