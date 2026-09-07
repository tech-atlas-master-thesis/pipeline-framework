import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.database import AsyncDatabase

SNAPSHOT_COLLECTION = "_snapshots"

_SLUG_PATTERN = re.compile(r"[^a-z0-9]+")


def slugify_version(version: str) -> str:
    slug = _SLUG_PATTERN.sub("_", version.strip().lower()).strip("_")
    if not slug:
        raise ValueError(f'Version "{version}" contains no usable characters for a collection name')
    return slug


class DownloadRecord(BaseModel):
    version: str
    url: str
    path: Optional[str] = None
    bytes: int = 0
    sha256: Optional[str] = None
    etag: Optional[str] = None
    last_modified: Optional[str] = None
    downloaded_at: datetime


class ActiveSnapshot(BaseModel):
    collection: str
    version: str
    records: int
    activated_at: datetime


class SnapshotState(BaseModel):
    source: str
    active: Optional[ActiveSnapshot] = None
    last_download: Optional[DownloadRecord] = None
    history: List[str] = Field(default_factory=list)


class SnapshotStore:
    def __init__(self, db: AsyncDatabase):
        self.db = db
        self.collection: AsyncCollection = db[SNAPSHOT_COLLECTION]

    @staticmethod
    def collection_name(source: str, version: str) -> str:
        return f"{source}__{slugify_version(version)}"

    async def get(self, source: str) -> Optional[SnapshotState]:
        document = await self.collection.find_one({"_id": source})
        if document is None:
            return None
        return SnapshotState.model_validate({**document, "source": document["_id"]})

    async def record_download(self, source: str, record: DownloadRecord) -> None:
        await self.collection.update_one(
            {"_id": source},
            {"$set": {"last_download": record.model_dump(mode="python")}},
            upsert=True,
        )

    async def activate(self, source: str, collection: str, version: str, records: int) -> ActiveSnapshot:
        active = ActiveSnapshot(
            collection=collection,
            version=version,
            records=records,
            activated_at=datetime.now(timezone.utc),
        )
        # Single update, so a swap is atomic and a failed load leaves the previous snapshot serving.
        await self.collection.update_one(
            {"_id": source},
            {"$set": {"active": active.model_dump(mode="python")}, "$addToSet": {"history": collection}},
            upsert=True,
        )
        return active

    async def require_active(self, source: str) -> ActiveSnapshot:
        state = await self.get(source)
        if state is None or state.active is None:
            raise FileNotFoundError(f'No active snapshot for "{source}". Run the registry pipeline first.')
        return state.active

    async def resolve(self, source: str) -> AsyncCollection:
        # Resolve once per pipeline run and pass the collection down, so a run stays on one
        # snapshot even if a refresh activates a new one while it is still going.
        return self.db[(await self.require_active(source)).collection]

    async def prune(self, source: str, keep: int = 2) -> List[str]:
        state = await self.get(source)
        if state is None:
            return []
        active_name = state.active.collection if state.active else None
        existing = set(await self.db.list_collection_names())

        history = [name for name in state.history if name in existing]
        keep_names = set(history[-keep:]) | ({active_name} if active_name else set())
        dropped = [name for name in history if name not in keep_names]
        for name in dropped:
            await self.db.drop_collection(name)

        remaining = [name for name in state.history if name not in dropped]
        await self.collection.update_one({"_id": source}, {"$set": {"history": remaining}})
        return dropped

    async def describe(self) -> Dict[str, Any]:
        summary: Dict[str, Any] = {}
        async for document in self.collection.find():
            state = SnapshotState.model_validate({**document, "source": document["_id"]})
            summary[state.source] = {
                "version": state.active.version if state.active else None,
                "collection": state.active.collection if state.active else None,
                "records": state.active.records if state.active else 0,
                "activated_at": state.active.activated_at.isoformat() if state.active else None,
            }
        return summary
