import json
from typing import Optional, Union, List, Dict, Any

import gridfs
import pandas as pd
from bson import ObjectId

from ..api import StepResultType
from ..config import StepConfig, LocalisationStringType, LocalisationString, UserStepConfig, StepUserConfig, EventType
from ..db import get_pipeline_db_client, get_raw_db_client
from ..db.helper import get_file_from_db


class GetResultFromLatestPipeline(StepConfig):
    def __init__(
        self,
        name: str = "getResultFromLatestPipeline",
        display_name: LocalisationStringType = LocalisationString(
            "Get Results from latest pipeline", "Get Results from latest pipeline"
        ),
        description: Optional[LocalisationStringType] = None,
        default_pipeline_name: Optional[str] = None,
        default_pipeline_step: Optional[str] = None,
    ):
        self._name = name
        self._description = description
        self._display_name = display_name
        self._default_pipeline_name = default_pipeline_name
        self._default_pipeline_step = default_pipeline_step

    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None, **_):
        PIPELINE_NAME = user_config.get("PIPELINE_NAME")
        PIPELINE_STEP = user_config.get("PIPELINE_STEP")

        pipeline_results = await self._get_pipeline_results(PIPELINE_NAME, PIPELINE_STEP)
        if pipeline_results is None:
            raise FileNotFoundError(
                f'No result returned for step "{PIPELINE_STEP}" in pipeline "{PIPELINE_NAME}" found'
            )
        yield pipeline_results, EventType.RESULT

    async def _get_pipeline_results(self, pipeline_name: str, step_name: str) -> Optional[Any]:
        pipeline_db = get_pipeline_db_client()
        pipeline = [*pipeline_db.get_collection("pipelines").find({"name": pipeline_name}).sort("_id", -1).limit(1)]
        if not pipeline:
            raise FileNotFoundError(f'No pipeline with name "{pipeline_name}" not found')
        step = pipeline_db.get_collection("steps").find_one({"name": step_name, "pipeline": pipeline[0]["_id"]})
        if not step:
            raise FileNotFoundError(f'No pipeline step with name "{step_name}" for pipeline "{pipeline_name} found')
        result = step["result"]
        if not result:
            raise FileNotFoundError(f'Pipeline step "{step_name}" for pipeline "{pipeline_name}" has no result')
        if not result["preview"]:
            return result["data"]
        if not result["file"]:
            raise FileNotFoundError(f"No file id found")
        file_data = get_file_from_db(ObjectId(result["file"]))
        match (result["type"]):
            case StepResultType.CSV:
                return pd.read_csv(file_data)
            case StepResultType.JSON:
                return json.load(file_data)
            case _:
                return file_data

    def user_config(self) -> List[StepUserConfig]:
        return [
            StepUserConfig(
                "PIPELINE_NAME",
                LocalisationString("Scraper Pipeline Name", "Scraper Pipeline Name"),
                LocalisationString("", ""),
                StepUserConfig.StepUserConfigType.PIPELINE,
                self._default_pipeline_name,
                pipelineType="scraper_main",
            ),
            StepUserConfig(
                "PIPELINE_STEP",
                LocalisationString("Scraper Pipeline Step", "Scraper Pipeline Step"),
                LocalisationString("", ""),
                StepUserConfig.StepUserConfigType.STEP,
                self._default_pipeline_step,
                pipelineType="scraper_main",
            ),
        ]

    def name(self) -> str:
        return self._name

    def display_name(self) -> LocalisationStringType:
        return self._display_name

    def description(self) -> LocalisationStringType:
        return self._description

    def dependencies(self) -> Union[List[str], None]:
        return None
