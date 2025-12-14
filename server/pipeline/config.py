from abc import abstractmethod, ABCMeta
from typing import List, TypedDict, Union


class PipelineConfig(TypedDict):
    steps: List[StepConfig]

class StepConfig(metaclass=ABCMeta):
    dependencies: List[str]

    @abstractmethod
    async def run(self):
        raise NotImplementedError("Execution function not implemented")

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplemented("Name not implemented")

    @property
    @abstractmethod
    def display_name(self):
        raise NotImplemented("Name not implemented")

    @property
    def dependencies(self) -> Union[List[str], None]:
        return None