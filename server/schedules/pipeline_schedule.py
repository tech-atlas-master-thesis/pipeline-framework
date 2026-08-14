from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any

from bson import ObjectId

from ..dto import AuditInfoDto
from ..dto.pipeline import PipelineCreation


@dataclass
class PipelineSchedule:
    id: str
    pipeline: PipelineCreation
    active: bool
    cron: Optional[str]
    created: AuditInfoDto
    modified: Optional[AuditInfoDto] = None
    lastExecution: Optional[datetime] = None
    lastPipeline: Optional[str] = None

    @classmethod
    def from_entity(cls, entity: Dict[str, Any]) -> "PipelineSchedule":
        return PipelineSchedule(
            id=str(entity["_id"]),
            pipeline=PipelineCreation.model_validate(entity["pipeline"]),
            cron=entity["cron"],
            active=entity["active"],
            created=AuditInfoDto.from_entity(entity["created"]),
            modified=AuditInfoDto.from_entity(entity["modified"]) if entity["modified"] else None,
            lastExecution=entity["lastExecution"],
            lastPipeline=entity["lastPipeline"],
        )

    def to_entity(self) -> Dict[str, Any]:
        return {
            "_id": ObjectId(self.id),
            "pipeline": self.pipeline.model_dump(),
            "cron": self.cron,
            "active": self.active,
            "created": self.created.serialize(),
            "modified": self.modified.serialize() if self.modified else None,
            "lastExecution": self.lastExecution,
            "lastPipeline": self.lastPipeline,
        }

    def serialize(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "pipeline": self.pipeline.model_dump(),
            "cron": self.cron,
            "active": self.active,
            "created": self.created.serialize(),
            "modified": self.modified.serialize() if self.modified else None,
            "lastExecution": self.lastExecution,
            "lastPipeline": self.lastPipeline,
        }
