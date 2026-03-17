from enum import Enum


class PipelineState(Enum):
    OPEN = "OPEN"
    RUNNING = "RUNNING"
    ERROR = "ERROR"
    FINISHED = "FINISHED"


class EventType(Enum):
    INFO = "INFO"
    ERROR = "ERROR"
    WARNING = "WARNING"
    DEBUG = "DEBUG"
    RESULT = "RESULT"
