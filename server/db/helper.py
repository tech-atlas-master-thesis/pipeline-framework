import os
from dataclasses import dataclass

from pymongo import MongoClient


@dataclass
class DatabaseLogin:
    database_name: str
    username: str
    password: str


def get_pipeline_db_client():
    return _get_pipeline_client(
        os.environ.get("MONGO_URL"),
        DatabaseLogin(os.environ.get("DB_PIPE_NAME"), os.environ.get("DB_PIPE_USER"), os.environ.get("DB_PIPE_PASS")),
    )


def get_raw_db_client():
    return _get_pipeline_client(
        os.environ.get("MONGO_URL"),
        DatabaseLogin(os.environ.get("DB_RAW_NAME"), os.environ.get("DB_RAW_USER"), os.environ.get("DB_RAW_PASS")),
    )


def _get_pipeline_client(mongo_db_url: str, login: DatabaseLogin):
    return MongoClient(
        # f"mongodb://{login.username}:{login.password}@{mongo_db_url}/{login.database_name}&authSource=admin"
        f"mongodb://{login.username}:{login.password}@{mongo_db_url}/{login.database_name}?authSource={login.database_name}"
        # f"mongodb://pipe_rw:12345678@{mongo_db_url}/pipelines?authSource=pipelines"
        # f"mongodb://root:12345678@{mongo_db_url}"
    )[login.database_name]
