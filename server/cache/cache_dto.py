from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Generic, Optional, TypeVar

from pydantic import BaseModel, ConfigDict, field_serializer

ResultT = TypeVar("ResultT", bound=BaseModel)


def normalize_key(value: str) -> str:
    return " ".join(value.strip().lower().split())


class CacheStatus(str, Enum):
    FOUND = "found"
    NOT_FOUND = "not_found"


class CacheKey(BaseModel):
    model_config = ConfigDict(frozen=True)

    query_type: str
    key: str
    match_version: int

    @classmethod
    def build(cls, query_type: str, query: str, match_version: int) -> "CacheKey":
        return cls(query_type=query_type, key=normalize_key(query), match_version=match_version)

    def as_filter(self) -> dict:
        return self.model_dump()


@dataclass(frozen=True)
class FetchResult(Generic[ResultT]):
    result: Optional[ResultT] = None
    source: Optional[dict] = None

    @property
    def status(self) -> CacheStatus:
        return CacheStatus.FOUND if self.result is not None else CacheStatus.NOT_FOUND

    @classmethod
    def not_found(cls, source: Optional[dict] = None) -> "FetchResult[ResultT]":
        return cls(result=None, source=source)


class CacheEntry(BaseModel, Generic[ResultT]):
    model_config = ConfigDict(extra="ignore")

    query_type: str
    key: str
    match_version: int
    query: str
    status: CacheStatus
    result: Optional[ResultT] = None
    source: Optional[dict] = None
    fetched_at: datetime

    @property
    def is_hit(self) -> bool:
        return self.status is CacheStatus.FOUND

    @field_serializer("status")
    def _serialize_status(self, status: CacheStatus) -> str:
        return status.value
