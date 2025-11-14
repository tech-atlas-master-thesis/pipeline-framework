from server import *

class BasicFirstStep(Step):
    def name(self) -> str:
        return "Basic First Step"
    async def run(self):
        print("Basic First Step")

class BasicSecondStep(Step):
    def name(self) -> str:
        return "Basic Second Step"
    async def run(self):
        print("Basic Second Step")

def main():
    pipeline_server = PipelineServer()

    pipeline = Pipeline.pipeline_with_consecutive_steps("Basic Pipeline", [BasicFirstStep(), BasicSecondStep()])

    pipeline_server.add_pipeline(pipeline)


if __name__ == "__main__":
    main()
