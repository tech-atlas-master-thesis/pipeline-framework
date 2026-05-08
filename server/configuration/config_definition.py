from dataclasses import dataclass
from typing import Any


from ..config import LocalisationStringType


@dataclass
class Configuration:
    type: str
    name: LocalisationStringType
    description: LocalisationStringType
    default_config: Any
