from dataclasses import dataclass
from typing import Dict

from pydantic import BaseModel

from .helper import get
from .pipeline import PipelineDto, PipelineCreation


class ScheduleCreation(BaseModel):
    pipeline: PipelineCreation
    active: bool
    cron: str

    def to_pipeline(self) -> PipelineCreation:
        return PipelineCreation(
            type=self.pipeline.type,
            name=self.pipeline.name,
            description=self.pipeline.description,
            config=self.pipeline.config,
        )


@dataclass
class ScheduleDto(PipelineDto):
    active: bool
    cron: str

    @classmethod
    def from_entity(cls, entity: Dict):
        pipeline = super().from_entity(entity)
        return cls(
            pipeline.id,
            pipeline.type,
            pipeline.name,
            pipeline.description,
            pipeline.state,
            pipeline.userConfig,
            pipeline.created,
            active=get(entity, "active", bool),
            cron=get(entity, "cron", str),
        )
