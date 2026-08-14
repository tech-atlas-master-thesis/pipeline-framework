from dataclasses import dataclass
from typing import Optional, Dict
from pydantic import BaseModel

from .helper import get
from .dto import AuditInfoDto
from ..config import (
    PipelineConfig,
    LocalisationStringType,
    UserConfig,
    PipelineState,
)


class PipelineCreation(BaseModel):
    type: str
    name: str
    description: str
    config: Optional[UserConfig] = None


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
    scheduleId: Optional[str]

    @classmethod
    def from_entity(cls, entity: Dict):
        return cls(
            get(entity, "_id", str),
            get(entity, "type"),
            get(entity, "name"),
            get(entity, "description"),
            get(entity, "state"),
            get(entity, "userConfig"),
            get(entity, "created"),
            get(entity, "scheduleId"),
        )
