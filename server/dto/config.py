from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional

from bson import ObjectId

from .dto import AuditInfoDto
from ..config import LocalisationStringType
from .helper import get


class ConfigurationState(str, Enum):
    DRAFT = "DRAFT"
    ACTIVE = "ACTIVE"
    ARCHIVED = "ARCHIVED"


@dataclass
class ConfigurationDefinitionDto:
    type: str
    name: LocalisationStringType
    description: Optional[LocalisationStringType] = None


@dataclass
class ConfigurationDto:
    id: str
    type: str
    name: Optional[str]
    description: Optional[str]
    created: AuditInfoDto
    modified: Optional[AuditInfoDto]

    @classmethod
    def from_entity(cls, entity: Dict):
        return cls(
            get(entity, "_id", str),
            get(entity, "type"),
            get(entity, "name"),
            get(entity, "description"),
            get(entity, "created"),
            get(entity, "modified"),
        )

    def to_entity(self):
        return {
            "_id": ObjectId(self.id),
            "type": self.type,
            "name": self.name,
            "description": self.description,
            "created": self.created.serialize(),
            "modified": self.modified.serialize() if self.modified else None,
        }


@dataclass
class ConfigurationVersionDto:
    id: str
    collection: str
    version: int
    name: Optional[str]
    description: Optional[str]
    state: ConfigurationState
    configuration: Any
    created: AuditInfoDto
    modified: Optional[AuditInfoDto]

    @classmethod
    def from_entity(cls, entity: Dict):
        return cls(
            get(entity, "_id", str),
            get(entity, "collection", str),
            get(entity, "version"),
            get(entity, "name"),
            get(entity, "description"),
            get(entity, "state"),
            get(entity, "configuration"),
            get(entity, "created"),
            get(entity, "modified"),
        )

    def to_entity(self):
        return {
            "_id": ObjectId(self.id),
            "collection": ObjectId(self.collection),
            "version": self.version,
            "name": self.name,
            "description": self.description,
            "state": self.state,
            "configuration": self.configuration,
            "created": self.created.serialize(),
            "modified": self.modified.serialize() if self.modified else None,
        }


@dataclass
class CreateConfigurationDto:
    type: str
    name: Optional[str]
    description: Optional[str]
    baseVersionId: Optional[str]


@dataclass
class UpdateConfigurationDto:
    name: Optional[str]
    description: Optional[str]


@dataclass
class UpdateConfigurationVersionDto:
    name: Optional[str]
    description: Optional[str]
    state: ConfigurationState
    configuration: Any
