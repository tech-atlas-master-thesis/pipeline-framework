from enum import Enum


class PipelineState(str, Enum):
    OPEN = "OPEN"
    RUNNING = "RUNNING"
    ERROR = "ERROR"
    FINISHED = "FINISHED"


class EventType(str, Enum):
    INFO = "INFO"
    ERROR = "ERROR"
    WARNING = "WARNING"
    DEBUG = "DEBUG"
    RESULT = "RESULT"
