import os
from dataclasses import dataclass

import gridfs
from bson import ObjectId
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


def get_file_from_db(file_id: ObjectId):
    file_db = gridfs.GridFS(get_raw_db_client())
    file = file_db.get(file_id)
    if not file:
        raise FileNotFoundError(f'No file with id "{file_id}" found')
    return file


def _get_pipeline_client(mongo_db_url: str, login: DatabaseLogin):
    return MongoClient(
        f"mongodb://{login.username}:{login.password}@{mongo_db_url}/{login.database_name}?authSource={login.database_name}"
    )[login.database_name]
