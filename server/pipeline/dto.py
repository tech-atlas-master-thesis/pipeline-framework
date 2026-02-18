from dataclasses import dataclass
from typing import List, Any

from pipelineFramework.server.pipeline.status import PipelineState


@dataclass
class PipelineDto:
    id: int
    name: str
    state: PipelineState
    display_name: str

@dataclass
class StepDto:
    id: int
    state: PipelineState
    name: str
    displayName: str
    events: List
    result: Any
