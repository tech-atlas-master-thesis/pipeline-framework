from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Generic, TypeVar, Dict


@dataclass
class PageDto:
    first: int
    rows: int
    totalRecords: int


T = TypeVar("T")


@dataclass
class PaginatedListDto(Generic[T]):
    items: List[T]
    page: PageDto


@dataclass
class AuditInfoDto:
    by: UserDto
    at: datetime

    def serialize(self) -> dict:
        return {
            "by": self.by.serialize(),
            "at": self.at,
        }

    @classmethod
    def from_entity(cls, entity: Dict) -> AuditInfoDto:
        return cls(
            by=UserDto.from_entity(entity["by"]),
            at=entity["at"],
        )


@dataclass
class UserDto:
    id: str | int
    name: str
    email: Optional[str] = None

    def serialize(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "email": self.email,
        }

    @classmethod
    def from_entity(cls, entity: Dict) -> UserDto:
        return cls(id=entity["id"], name=entity["name"], email=entity["email"])
