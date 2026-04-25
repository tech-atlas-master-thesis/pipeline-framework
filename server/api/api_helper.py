from typing import List

from fastapi import FastAPI

from pipelineFramework.server.config import PipelineConfig
from pipelineFramework.server.server import PipelineServer
from . import pipeline_api, step_api, config_api


def add_common_api_calls(
    app: FastAPI, pipeline_server: PipelineServer, pipeline_config: List[PipelineConfig], api_base_url: str
) -> None:
    @app.get(api_base_url + "/hello-world/")
    async def hello_world():
        return {"message": "Hello World"}

    config_api.config_enpoints(app, pipeline_config, api_base_url)
    pipeline_api.pipeline_endpoints(app, pipeline_server, pipeline_config, api_base_url)
    step_api.step_endpoints(app, api_base_url)
