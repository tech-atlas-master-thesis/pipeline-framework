from typing import List

from fastapi import FastAPI

from ..server import PipelineConfig, PipelineServer
from ..configuration import Configuration
from . import pipeline_api, step_api, config_api, configuration_api


def add_common_api_calls(
    app: FastAPI,
    pipeline_server: PipelineServer,
    api_base_url: str,
) -> None:
    @app.get(api_base_url + "/hello-world/")
    async def hello_world():
        return {"message": "Hello World"}

    config_api.config_endpoints(app, pipeline_server, api_base_url)
    pipeline_api.pipeline_endpoints(app, pipeline_server, api_base_url)
    step_api.step_endpoints(app, api_base_url)
    configuration_api.configuration_endpoints(app, pipeline_server, api_base_url)
