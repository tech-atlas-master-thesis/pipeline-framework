import asyncio
from collections.abc import Callable

from server import *


async def check_running(pipeline_server: PipelineServer, timeout: float):
    async with asyncio.timeout(timeout):
        while True:
            await asyncio.sleep(1)
            if all(pipeline.state == PipelineState.FINISHED for pipeline in pipeline_server.pipelines):
                return True


def execute_test(test_runner: Callable[PipelineServer, None], test_check: Callable[PipelineServer, bool] = lambda _: True, timeout: float = 5):
    pipeline_server = PipelineServer()
    event_loop = asyncio.new_event_loop()

    _ = event_loop.create_task(test_runner(pipeline_server), name="Test runner")
    task = event_loop.create_task(check_running(pipeline_server, timeout), name="Test Checker")
    event_loop.run_until_complete(task)
    test_check(pipeline_server)