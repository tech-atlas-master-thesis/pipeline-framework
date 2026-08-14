from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional, List, Dict

from .helper import get
from ..config import (
    StepConfig,
    StepUserConfig,
    LocalisationStringType,
    PipelineState,
    Event,
)


@dataclass
class StepConfigDto:
    name: str
    displayName: LocalisationStringType
    description: Optional[LocalisationStringType] = None
    userConfig: List[StepUserConfig] = field(default_factory=list)
    dependencies: Optional[List[str]] = None

    def __init__(self, step: StepConfig):
        self.name = step.name()
        self.displayName = step.display_name()
        self.userConfig = step.user_config()
        self.dependencies = step.dependencies()


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
    dependencies: List[str]

    @classmethod
    def from_entity(cls, entity: Dict):
        return cls(
            get(entity, "_id", str),
            get(entity, "state"),
            get(entity, "name"),
            get(entity, "displayName"),
            get(entity, "description"),
            get(entity, "events"),
            get(entity, "result"),
            get(entity, "dependencies"),
        )
