from dataclasses import dataclass
from typing import Any, Optional

from ..config import LocalisationStringType


@dataclass
class Configuration:
    type: str
    name: LocalisationStringType
    default_config: Any
    description: Optional[LocalisationStringType] = None
