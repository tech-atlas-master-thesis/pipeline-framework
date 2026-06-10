from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any

from ..config import UserConfig
from ..dto import AuditInfoDto, PipelineDto


@dataclass
class PipelineSchedule:
    id: str
    type: str
    name: str
    description: str
    active: bool
    cron: Optional[str]
    config: Optional[UserConfig]
    created: AuditInfoDto
    modified: AuditInfoDto
    lastExecution: datetime
    lastPipeline: str

    @classmethod
    def from_entity(cls, entity: Dict[str, Any]) -> "PipelineSchedule":
        return PipelineSchedule(
            id=entity["_id"],
            type=entity["type"],
            name=entity["name"],
            description=entity["description"],
            cron=entity["cron"],
            config=entity["config"],
            created=entity["created"],
            modified=entity["modified"],
            lastExecution=entity["lastExecution"],
            lastPipeline=entity["lastPipeline"],
        )

    def serialize(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "type": self.type,
            "name": self.name,
            "description": self.description,
            "cron": self.cron,
            "config": self.config,
            "created": self.created.serialize(),
            "modified": self.modified.serialize(),
            "lastExecution": self.lastExecution,
            "lastPipeline": self.lastPipeline,
        }
