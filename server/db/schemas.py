import dataclasses
import json
from dataclasses import dataclass
from typing import Optional, List

from bson import ObjectId

from ..config import PipelineState, UserConfig
from ..dto import StepResultType, Event


@dataclass
class PipelineEntity:
    id: ObjectId
    type: str
    name: str
    description: str
    state: PipelineState
    userConfig: Optional[UserConfig]

    def to_json(self):
        return json.dumps(dataclasses.asdict(self))


@dataclass
class StepResultEntity:
    type: StepResultType
    preview: bool
    file: str

    def to_json(self):
        return json.dumps(dataclasses.asdict(self))


@dataclass
class StepEntity:
    id: int
    state: PipelineState
    name: str
    events: List[Event]
    result: StepResultEntity

    def to_json(self):
        return json.dumps(dataclasses.asdict(self))
