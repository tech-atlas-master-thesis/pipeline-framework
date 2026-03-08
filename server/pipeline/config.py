import datetime
from abc import abstractmethod, ABCMeta
from dataclasses import dataclass
from enum import Enum
from typing import List, Union, Optional, Dict

from pipelineFramework.server.api import LocalisationString, UserStepConfig

UserConfigValue = Union[str, int, float, Dict[str, str], List[str], datetime.datetime]

@dataclass
class StepUserConfig:
    class StepUserConfigType(Enum):
        STRING = "STRING"
        INTEGER = "INTEGER"
        FLOAT = "FLOAT"
        LIST = "LIST"
        MAPPING = "MAPPING"
        ENUM = "ENUM"
        DATE = "DATE"

    name: str
    displayValue: LocalisationString
    type: StepUserConfigType
    defaultValue: Optional[UserConfigValue] = None
    enumValue: Optional[List[str]] = None


class StepConfig(metaclass=ABCMeta):
    @abstractmethod
    async def run(self, user_config: Optional[UserStepConfig]):
        yield
        raise NotImplementedError("Execution function not implemented")

    @abstractmethod
    def name(self) -> str:
        raise NotImplemented("Name not implemented")

    @abstractmethod
    def display_name(self) -> LocalisationString:
        raise NotImplemented("Name not implemented")

    def description(self) -> Optional[LocalisationString]:
        return None

    def user_config(self) -> List[StepUserConfig]:
        return []

    def dependencies(self) -> Union[List[str], None]:
        return None

@dataclass
class PipelineConfig:
    name: str
    display_name: LocalisationString
    parallelize: bool
    steps: List[StepConfig]
    description: Optional[LocalisationString]
