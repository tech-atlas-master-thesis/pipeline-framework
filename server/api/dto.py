from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Any

from pydantic import BaseModel

from ..config import PipelineConfig, StepConfig, StepUserConfig, \
    LocalisationString, UserConfig, PipelineState
from ..config.config import LocalisationStringType


class PipelineCreation(BaseModel):
    name: str
    config: Optional[UserConfig] = None


@dataclass
class StepConfigDto:
    name: str
    displayName: LocalisationStringType
    userConfig: List[StepUserConfig]

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
    displayName: str


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
    displayName: str
    events: List
    result: StepResultDto
