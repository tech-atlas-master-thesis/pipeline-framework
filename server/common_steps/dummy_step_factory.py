from typing import Union, List

from ..config import StepConfig, EventType, LocalisationStringType


def get_dummy_step(name: str, display_name: LocalisationStringType):
    class DummyStep(StepConfig):
        async def run(self, **_):
            yield "Dummy Step executed", EventType.INFO

        def name(self) -> str:
            return name

        def display_name(self) -> LocalisationStringType:
            return display_name

        def dependencies(self) -> Union[List[str], None]:
            return None

    return DummyStep()
