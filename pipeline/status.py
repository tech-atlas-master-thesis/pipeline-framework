from enum import Enum


class PipelineState(Enum):
    OPEN = "OPEN"
    RUNNING = "RUNNING"
    ERROR = "ERROR"
    FINISHED = "FINISHED"