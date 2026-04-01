from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import List, Optional, Any, Dict, Callable, Generic, TypeVar

from pydantic import BaseModel

from ..config import (
    PipelineConfig,
    StepConfig,
    StepUserConfig,
    LocalisationStringType,
    UserConfig,
    PipelineState,
    EventType,
)


def _get(obj: Dict[str, Any], key: str, transformer: Optional[Callable[[Any], Any]] = None) -> Any:
    if key not in obj:
        return None
    value = obj[key]
    if transformer:
        return transformer(value)
    return value


@dataclass
class Event:
    timestamp: datetime
    message: str
    type: EventType


class PipelineCreation(BaseModel):
    type: str
    name: str
    description: str
    config: Optional[UserConfig] = None


@dataclass
class StepConfigDto:
    name: str
    displayName: LocalisationStringType
    description: Optional[LocalisationStringType] = None
    userConfig: List[StepUserConfig] = field(default_factory=list)

    def __init__(self, step: StepConfig):
        self.name = step.name()
        self.displayName = step.display_name()
        self.userConfig = step.user_config()


@dataclass
class PipelineConfigDto:
    type: str
    displayName: LocalisationStringType
    description: LocalisationStringType

    def __init__(self, pipeline: PipelineConfig):
        self.type = pipeline.type
        self.displayName = pipeline.display_name
        self.description = pipeline.description


@dataclass
class PipelineDto:
    id: str
    type: str
    name: str
    description: str
    state: PipelineState
    userConfig: Optional[UserConfig]
    created: AuditInfoDto

    @classmethod
    def from_entity(cls, entity: Dict):
        return cls(
            _get(entity, "_id", str),
            _get(entity, "type"),
            _get(entity, "name"),
            _get(entity, "description"),
            _get(entity, "state"),
            _get(entity, "userConfig"),
            _get(entity, "created"),
        )


class StepResultType(str, Enum):
    STRING = "STRING"
    JSON = "JSON"
    CSV = "CSV"


@dataclass
class StepResultDto:
    type: StepResultType
    preview: bool
    file: str
    data: Any


@dataclass
class StepDto:
    id: str
    state: PipelineState
    name: str
    displayName: LocalisationStringType
    description: LocalisationStringType
    events: List[Event]
    result: StepResultDto

    @classmethod
    def from_entity(cls, entity: Dict):
        return cls(
            _get(entity, "_id", str),
            _get(entity, "state"),
            _get(entity, "name"),
            _get(entity, "displayName"),
            _get(entity, "description"),
            _get(entity, "events"),
            _get(entity, "result"),
        )


@dataclass
class PageDto:
    first: int
    rows: int
    totalRecords: int


T = TypeVar("T")


@dataclass
class PaginatedListDto(Generic[T]):
    items: List[T]
    page: PageDto


@dataclass
class AuditInfoDto:
    by: UserDto
    at: datetime

    def serialize(self) -> dict:
        return {
            "by": self.by.serialize(),
            "at": self.at,
        }


@dataclass
class UserDto:
    id: str | int
    name: str
    email: Optional[str] = None

    def serialize(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "email": self.email,
        }
