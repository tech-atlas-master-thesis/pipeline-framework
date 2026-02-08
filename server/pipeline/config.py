from abc import abstractmethod, ABCMeta
from dataclasses import dataclass
from typing import List, Union


@dataclass
class PipelineConfig:
    name: str
    display_name: str
    parallelize: bool
    steps: List[StepConfig]


class StepConfig(metaclass=ABCMeta):
    @abstractmethod
    async def run(self):
        yield
        raise NotImplementedError("Execution function not implemented")

    @abstractmethod
    def name(self) -> str:
        raise NotImplemented("Name not implemented")

    @abstractmethod
    def display_name(self):
        raise NotImplemented("Name not implemented")

    def dependencies(self) -> Union[List[str], None]:
        return None