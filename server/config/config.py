import datetime
from abc import abstractmethod, ABCMeta
from dataclasses import dataclass
from enum import Enum
from typing import List, Union, Optional, Dict, Any

UserConfigValue = Union[str, int, float, Dict[str, str], List[str], datetime.datetime]

UserStepConfig = Dict[str, UserConfigValue]
UserConfig = Dict[str, UserStepConfig]


@dataclass
class LocalisationString:
    en: str
    de: str

    def to_json(self) -> Dict[str, Any]:
        return {"en": self.en, "de": self.de}


LocalisationStringType = LocalisationString | str


@dataclass
class UserConfigEnumDto:
    name: str
    displayName: LocalisationStringType
    description: LocalisationStringType


@dataclass
class StepUserConfig:
    class StepUserConfigType(str, Enum):
        STRING = "STRING"
        INTEGER = "INTEGER"
        FLOAT = "FLOAT"
        LIST = "LIST"
        MAPPING = "MAPPING"
        ENUM = "ENUM"
        DATE = "DATE"
        PIPELINE = "PIPELINE"
        STEP = "STEP"

    name: str
    displayName: LocalisationStringType
    description: Optional[LocalisationStringType]
    type: StepUserConfigType
    defaultValue: Optional[UserConfigValue] = None
    enumValues: Optional[List[UserConfigEnumDto]] = None
    pipelineType: Optional[str] = None


class StepConfig(metaclass=ABCMeta):
    @abstractmethod
    async def run(self, user_config: Optional[UserStepConfig] = None, results: Optional[Dict[str, Any]] = None):
        raise NotImplementedError

    @abstractmethod
    def name(self) -> LocalisationStringType:
        raise NotImplemented("Name not implemented")

    @abstractmethod
    def display_name(self) -> LocalisationStringType:
        raise NotImplemented("Name not implemented")

    def description(self) -> Optional[LocalisationStringType]:
        return None

    def user_config(self) -> List[StepUserConfig]:
        return []

    def dependencies(self) -> Union[List[str], None]:
        return None


@dataclass
class PipelineConfig:
    type: str
    display_name: LocalisationStringType
    parallelize: bool
    steps: List[StepConfig]
    description: Optional[LocalisationStringType] = None
