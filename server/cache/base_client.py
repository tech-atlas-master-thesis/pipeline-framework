from abc import ABC, abstractmethod
from typing import Generic, Optional

from .cache import EnrichmentCache, ProviderCache
from .cache_dto import CacheEntry, FetchResult, ResultT


class BaseEnrichmentClient(ABC, Generic[ResultT]):
    provider: str
    query_type: str
    result_model: type[ResultT]
    match_version: int = 1

    def __init__(self, cache: EnrichmentCache):
        self.cache: ProviderCache[ResultT] = cache.for_provider(
            self.provider,
            self.query_type,
            self.result_model,
            self.match_version,
        )

    async def ensure_indexes(self) -> None:
        await self.cache.ensure_indexes()

    async def lookup(self, query: str) -> Optional[ResultT]:
        return await self.cache.lookup(query, self._fetch)

    async def lookup_entry(self, query: str) -> CacheEntry[ResultT]:
        return await self.cache.lookup_entry(query, self._fetch)

    @abstractmethod
    async def _fetch(self, query: str) -> FetchResult[ResultT]:
        """Call the external API for `query`.

        Return `FetchResult(result=..., source=...)` on a match, or
        `FetchResult.not_found(source=...)` when the API genuinely reports no
        match (that gets cached).

        Let exceptions (timeouts, HTTP errors, rate limiting) propagate - do
        not catch them and return `not_found`, or you will permanently cache a
        transient failure.
        """
        raise NotImplementedError
