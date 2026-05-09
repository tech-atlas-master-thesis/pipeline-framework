from typing import List, Optional, Annotated

from fastapi import FastAPI, Query
from fastapi.params import Depends

from .authentication import require_all_entitlements
from ..configuration import ConfigurationManager, Configuration
from ..dto import (
    ConfigurationDto,
    CreateConfigurationDto,
    ConfigurationVersionDto,
    PaginatedListDto,
    ConfigurationDefinitionDto,
    UpdateConfigurationDto,
    UpdateConfigurationVersionDto,
)

AUTH_REQUIREMENTS_VIEW = require_all_entitlements("tech-atlas:read")
AUTH_REQUIREMENTS_EDIT = require_all_entitlements("tech-atlas:write")


def configuration_endpoints(app: FastAPI, config_definitions: List[Configuration], api_base_url: str):
    config_manager = ConfigurationManager(config_definitions)

    @app.get(api_base_url + "/configuration-types")
    async def get_configuration_types() -> List[ConfigurationDefinitionDto]:
        return config_manager.get_configuration_definition()

    @app.get(api_base_url + "/configuration")
    async def get_configurations(
        type: Annotated[Optional[List[str]], Query()] = None,
        name: Optional[str] = None,
        sort: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
        _=Depends(AUTH_REQUIREMENTS_VIEW),
    ) -> PaginatedListDto[ConfigurationDto]:
        return config_manager.get_configurations(type, name, sort, limit, offset)

    @app.post(api_base_url + "/configuration")
    async def create_configuration(
        body: CreateConfigurationDto, user=Depends(AUTH_REQUIREMENTS_EDIT)
    ) -> ConfigurationDto:
        return config_manager.create_new_configuration(body.type, body.name, body.description, user)

    @app.get(api_base_url + "/configuration/{configuration_id}")
    async def get_configuration(configuration_id: str, _=Depends(AUTH_REQUIREMENTS_VIEW)) -> ConfigurationDto:
        return config_manager.get_configuration(configuration_id)

    @app.post(api_base_url + "/configuration/{configuration_id}")
    async def update_configuration(
        configuration_id: str, configuration: UpdateConfigurationDto, user=Depends(AUTH_REQUIREMENTS_EDIT)
    ) -> ConfigurationDto:
        return config_manager.update_configuration(configuration_id, configuration, user)

    @app.get(api_base_url + "/configuration/{configuration_id}/version")
    def get_version(
        configuration_id: str,
        state: Annotated[Optional[List[str]], Query()] = None,
        name: Optional[str] = None,
        sort: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
        _=Depends(AUTH_REQUIREMENTS_VIEW),
    ) -> PaginatedListDto[ConfigurationVersionDto]:
        return config_manager.get_versions(configuration_id, state, name, sort, limit, offset)

    @app.post(api_base_url + "/configuration/{configuration_id}/version")
    async def create_version(
        body: CreateConfigurationDto, user=Depends(AUTH_REQUIREMENTS_EDIT)
    ) -> ConfigurationVersionDto:
        return config_manager.create_new_version(body.type, body.name, body.description, user)

    @app.get(api_base_url + "/configuration/{configuration_id}/version/{version_id}")
    def get_version(
        configuration_id: str, version_id: str, _=Depends(AUTH_REQUIREMENTS_VIEW)
    ) -> ConfigurationVersionDto:
        return config_manager.get_version(configuration_id, version_id)

    @app.post(api_base_url + "/configuration/{configuration_id}/version/{version_id}")
    def update_version(
        configuration_id: str, version_id, version: UpdateConfigurationVersionDto, user=Depends(AUTH_REQUIREMENTS_EDIT)
    ) -> ConfigurationVersionDto:
        return config_manager.update_version(configuration_id, version_id, version, user)
