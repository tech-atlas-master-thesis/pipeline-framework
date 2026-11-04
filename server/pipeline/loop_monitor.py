import asyncio
import logging
from typing import List

logger = logging.getLogger(__name__)

STEP_TASK_PREFIX = "step:"

CHECK_INTERVAL = 0.1
STALL_THRESHOLD = 0.2


def running_steps() -> List[str]:
    names = (task.get_name() for task in asyncio.all_tasks())
    return sorted(name.removeprefix(STEP_TASK_PREFIX) for name in names if name.startswith(STEP_TASK_PREFIX))


async def monitor_event_loop(interval: float = CHECK_INTERVAL, threshold: float = STALL_THRESHOLD) -> None:
    # Everything shares this loop, so a stall here delays every request and pipeline. CPU-heavy
    # work belongs in asyncio.to_thread; this names the steps to look at when something was missed.
    loop = asyncio.get_running_loop()
    while True:
        started = loop.time()
        await asyncio.sleep(interval)
        stall = loop.time() - started - interval
        if stall >= threshold:
            steps = ", ".join(running_steps()) or "none"
            logger.warning(f"Event loop stalled for {stall * 1000:.0f} ms (running steps: {steps})")
