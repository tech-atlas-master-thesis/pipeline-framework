from dataclasses import dataclass
from enum import Enum
from typing import List, Any

from pipelineFramework.server.pipeline.status import PipelineState


@dataclass
class PipelineDto:
    id: int
    name: str
    state: PipelineState
    displayName: str

@dataclass
class StepDto:
    id: int
    state: PipelineState
    name: str
    displayName: str
    events: List
    result: StepResultDto

class StepResultType(Enum):
    STRING = "STRING"
    JSON = "JSON"
    CSV = "CSV"

@dataclass
class StepResultDto:
    type: StepResultType
    preview: bool
    data: Any
