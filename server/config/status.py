from dataclasses import dataclass
from datetime import datetime, UTC
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


@dataclass
class Event:
    timestamp: datetime
    message: str
    type: EventType

    @classmethod
    def now(cls, message: str, type: EventType):
        return cls(datetime.now(UTC), message, type)
