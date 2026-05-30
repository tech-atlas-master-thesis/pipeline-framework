import datetime
import re
from typing import List, Dict, Optional

from bson import ObjectId
from fastapi import HTTPException

from ..dto import UserDto, AuditInfoDto, PaginatedListDto, PageDto
from .config_definition import Configuration
from ..db import get_pipeline_db_client
from ..dto import (
    ConfigurationDto,
    ConfigurationVersionDto,
    ConfigurationState,
    ConfigurationDefinitionDto,
    UpdateConfigurationDto,
    UpdateConfigurationVersionDto,
)


class ConfigurationManager:
    def __init__(self, config_definitions: List[Configuration]):
        self.config_db = get_pipeline_db_client().get_collection("configuration")
        self.version_db = get_pipeline_db_client().get_collection("configuration_version")
        self.definitions = {d.type: d for d in config_definitions}

    def get_configuration_definition(self) -> List[ConfigurationDefinitionDto]:
        return [ConfigurationDefinitionDto(d.type, d.name, d.description) for d in self.definitions.values()]

    def get_configurations(
        self,
        type: Optional[List[str]] = None,
        name: Optional[str] = None,
        sort: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> PaginatedListDto[ConfigurationDto]:
        query = {}
        if type:
            query["type"] = {"$in": type}
        if name:
            query["name"] = {"$regex": re.escape(name)}
        if sort:
            single_sorts = (single_sort.split(":") for single_sort in sort.split(";"))
            sort_query = {field: int(order) for field, order in single_sorts}
        else:
            sort_query = {"_id": -1}
        configs = self.config_db.find(query).sort(sort_query).skip(offset).limit(limit)
        total_records = self.config_db.count_documents(query)
        return PaginatedListDto(
            [ConfigurationDto.from_entity(config) for config in configs], PageDto(offset, limit, total_records)
        )

    def create_new_configuration(
        self, type: str, name: Optional[str], description: Optional[str], user: UserDto
    ) -> ConfigurationDto:
        if type not in self.definitions:
            raise FileNotFoundError(f'Definition for type "{type}" not found')
        config = ConfigurationDto(
            None, type, name, description, AuditInfoDto(user, datetime.datetime.now(datetime.UTC)), None
        )
        new_id = self.config_db.insert_one(config.to_entity())
        config.id = str(new_id.inserted_id)
        return config

    def get_configuration(self, collection_id: str) -> ConfigurationDto:
        collection = self.config_db.find_one({"_id": ObjectId(collection_id)})
        if collection is None:
            raise FileNotFoundError(f'Collection with id "{collection_id}" not found')
        return ConfigurationDto.from_entity(collection)

    def update_configuration(self, config_id: str, config: UpdateConfigurationDto, user: UserDto) -> ConfigurationDto:
        self.config_db.update_one(
            {"_id": ObjectId(config_id)},
            {
                "$set": {
                    "name": config.name,
                    "description": config.description,
                    "modified": AuditInfoDto(user, datetime.datetime.now(datetime.UTC)).serialize(),
                }
            },
        )

        return self.get_configuration(config_id)

    def get_versions(
        self,
        configuration_id: str,
        state: Optional[List[str]],
        name: Optional[str],
        sort: Optional[str],
        limit: int = 20,
        offset: int = 0,
    ) -> PaginatedListDto[ConfigurationVersionDto]:
        query: Dict = {"collection": ObjectId(configuration_id)}
        if state:
            query["state"] = {"$in": state}
        if name:
            query["name"] = {"$regex": re.escape(name)}
        if sort:
            single_sorts = (single_sort.split(":") for single_sort in sort.split(";"))
            sort_query = {field: int(order) for field, order in single_sorts}
        else:
            sort_query = {"_id": -1}
        versions = self.version_db.find(query).sort(sort_query).skip(offset).limit(limit)
        total_records = self.version_db.count_documents(query)
        return PaginatedListDto(
            [ConfigurationVersionDto.from_entity(version) for version in versions],
            PageDto(offset, limit, total_records),
        )

    def get_latest_version(self, configuration_id: str, state: Optional[List[str]] = None) -> ConfigurationVersionDto:
        query: Dict = {"collection": ObjectId(configuration_id)}
        if state:
            query["state"] = {"$in": state}
        sort_query = {"version": -1}
        version = self.version_db.find_one(query).sort(sort_query)
        return ConfigurationVersionDto.from_entity(version)

    def create_new_version(
        self,
        collection_id: str,
        name: Optional[str],
        description: Optional[str],
        base_version_id: Optional[str],
        user: UserDto,
    ) -> ConfigurationVersionDto:
        now = AuditInfoDto(user, datetime.datetime.now(datetime.UTC))
        base_version = self.version_db.find_one({"_id": ObjectId(base_version_id)}) if base_version_id else None
        if base_version_id and not base_version:
            raise HTTPException(status_code=404, detail="Base version not found")
        versions = self.version_db.count_documents({"collection": ObjectId(collection_id)})
        collection = self.config_db.find_one({"_id": ObjectId(collection_id)})
        if collection is None:
            raise HTTPException(status_code=404, detail=f'Collection with id "{collection}" not found')
        definition = self.definitions[collection["type"]]
        if definition is None:
            raise HTTPException(status_code=404, detail=f'Definition for "{name}" not found')
        version = ConfigurationVersionDto(
            None,
            collection_id,
            versions + 1,
            name,
            description,
            ConfigurationState.DRAFT,
            base_version["configuration"] if base_version else definition.default_config,
            now,
            None,
        )
        new_id = self.version_db.insert_one(version.to_entity())
        version.id = str(new_id.inserted_id)

        return version

    def get_version(self, collection_id: str, version_id: str) -> ConfigurationVersionDto:
        version = self.version_db.find_one({"_id": ObjectId(version_id), "collection": ObjectId(collection_id)})
        if version is None:
            raise FileNotFoundError(
                f'Collection with id "{version_id}" from collection with ID "{collection_id}" not found'
            )
        return ConfigurationVersionDto.from_entity(version)

    def update_version(
        self, config_id: str, version_id, version: UpdateConfigurationVersionDto, user: UserDto
    ) -> ConfigurationVersionDto:
        now = AuditInfoDto(user, datetime.datetime.now(datetime.UTC))
        self.version_db.update_one(
            {"_id": ObjectId(version_id), "collection": ObjectId(config_id)},
            {
                "$set": {
                    "name": version.name,
                    "description": version.description,
                    "state": version.state,
                    "configuration": version.configuration,
                    "modified": now.serialize(),
                }
            },
        )

        return self.get_version(config_id, version_id)
