from server import *


class BasicFirstStep(StepConfig):
    def display_name(self):
        return "Basic First Step"
    def name(self):
        return "basic-first-step"
    async def run(self):
        print("Basic First Step")

class BasicSecondStep(StepConfig):
    @property
    def display_name(self):
        return "Basic Second Step"
    def name(self) -> str:
        return "basic-second-step"
    async def run(self):
        print("Basic Second Step")

def main():
    pipeline_server = PipelineServer()

    pipeline: PipelineConfig = {
        'steps': [BasicFirstStep(), BasicSecondStep()],
    }

    pipeline_server.add_pipeline(pipeline)

    pipeline_server.start_server()


if __name__ == "__main__":
    main()
