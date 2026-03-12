from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import List, Optional, Any

from pydantic import BaseModel

from ..config import PipelineConfig, StepConfig, StepUserConfig, \
    LocalisationStringType, UserConfig, PipelineState, EventType


@dataclass
class Event:
    timestamp: datetime
    message: str
    type: EventType


class PipelineCreation(BaseModel):
    name: str
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
    name: str
    displayName: LocalisationStringType
    description: LocalisationStringType

    def __init__(self, pipeline: PipelineConfig):
        self.name = pipeline.name
        self.displayName = pipeline.display_name
        self.description = pipeline.description


@dataclass
class PipelineDto:
    id: int
    name: str
    state: PipelineState
    description: LocalisationStringType
    displayName: LocalisationStringType


class StepResultType(Enum):
    STRING = "STRING"
    JSON = "JSON"
    CSV = "CSV"


@dataclass
class StepResultDto:
    type: StepResultType
    preview: bool
    data: Any


@dataclass
class StepDto:
    id: int
    state: PipelineState
    name: str
    displayName: LocalisationStringType
    description: LocalisationStringType
    events: List[Event]
    result: StepResultDto
