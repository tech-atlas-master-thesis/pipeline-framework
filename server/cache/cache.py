from datetime import datetime, timezone
from typing import Awaitable, Callable, Generic, Optional

from bson.codec_options import CodecOptions
from pymongo import ASCENDING
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.database import AsyncDatabase

from .cache_dto import CacheEntry, CacheKey, CacheStatus, FetchResult, ResultT, normalize_key

FetchFn = Callable[[str], Awaitable[FetchResult[ResultT]]]


class ProviderCache(Generic[ResultT]):
    def __init__(
        self,
        collection: AsyncCollection,
        query_type: str,
        result_model: type[ResultT],
        match_version: int = 1,
    ):
        self.collection = collection
        self.query_type = query_type
        self.result_model = result_model
        self.match_version = match_version
        self._entry_model: type[CacheEntry[ResultT]] = CacheEntry[result_model]  # type: ignore[valid-type]

    def key_for(self, query: str) -> CacheKey:
        return CacheKey.build(self.query_type, query, self.match_version)

    async def ensure_indexes(self) -> None:
        await self.collection.create_index(
            [("query_type", ASCENDING), ("key", ASCENDING), ("match_version", ASCENDING)],
            unique=True,
            name="cache_key",
        )

    async def get(self, query: str) -> Optional[CacheEntry[ResultT]]:
        document = await self.collection.find_one(self.key_for(query).as_filter())
        if document is None:
            return None
        return self._entry_model.model_validate(document)

    async def put(self, query: str, fetched: FetchResult[ResultT]) -> CacheEntry[ResultT]:
        key = self.key_for(query)
        entry = self._entry_model(
            query_type=key.query_type,
            key=key.key,
            match_version=key.match_version,
            query=query,
            status=fetched.status,
            result=fetched.result,
            source=fetched.source,
            fetched_at=datetime.now(timezone.utc),
        )
        update = entry.model_dump(mode="python", exclude=set(CacheKey.model_fields))
        await self.collection.update_one(key.as_filter(), {"$set": update}, upsert=True)
        return entry

    async def lookup_entry(self, query: str, fetch_fn: FetchFn[ResultT]) -> CacheEntry[ResultT]:
        cached = await self.get(query)
        if cached is not None:
            return cached
        return await self.put(query, await fetch_fn(query))

    async def lookup(self, query: str, fetch_fn: FetchFn[ResultT]) -> Optional[ResultT]:
        entry = await self.lookup_entry(query, fetch_fn)
        return entry.result if entry.status is CacheStatus.FOUND else None


class EnrichmentCache:
    def __init__(self, db: AsyncDatabase):
        self.db = db

    def for_provider(
        self,
        provider: str,
        query_type: str,
        result_model: type[ResultT],
        match_version: int = 1,
    ) -> ProviderCache[ResultT]:
        return ProviderCache(
            self.db.get_collection(provider, CodecOptions(tz_aware=True)),
            query_type=query_type,
            result_model=result_model,
            match_version=match_version,
        )
