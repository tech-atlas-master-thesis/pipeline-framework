from abc import abstractmethod, ABCMeta
from typing import List, TypedDict, Union


class PipelineConfig(TypedDict):
    name: str
    display_name: str
    parallelize: bool | None
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