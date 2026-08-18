from datetime import datetime, timezone
from typing import Callable, Optional

from pymongo.asynchronous.database import AsyncDatabase


def normalize_key(value: str) -> str:
    return " ".join(value.strip().lower().split())


class EnrichmentCache:
    def __init__(self, db: AsyncDatabase):
        self.db = db

    async def get(self, provider: str, query_type: str, query: str, match_version: int) -> Optional[dict]:
        key = normalize_key(query)
        return await self.db.get_collection(provider).find_one(
            {
                "query_type": query_type,
                "key": key,
                "match_version": match_version,
            }
        )

    async def put(
        self,
        provider: str,
        query_type: str,
        query: str,
        match_version: int,
        status: str,
        result: Optional[dict],
        source: Optional[dict] = None,
    ) -> None:
        key = normalize_key(query)
        await self.db.get_collection(provider).update_one(
            {
                "query_type": query_type,
                "key": key,
                "match_version": match_version,
            },
            {
                "$set": {
                    "query": query,
                    "status": status,
                    "result": result,
                    "source": source,
                    "fetched_at": datetime.now(timezone.utc),
                }
            },
            upsert=True,
        )

    async def lookup(
        self,
        provider: str,
        query_type: str,
        query: str,
        fetch_fn: Callable[[str], tuple],
        match_version: int = 1,
    ) -> Optional[dict]:
        """Cache-aside lookup.

        fetch_fn(query) -> (result, source_metadata)
          - result is a dict on a genuine match, or None on a genuine "not found"
            response from the API (this gets cached - we don't want to keep
            asking the API about orgs it has already told us it doesn't know).
          - Exceptions raised by fetch_fn (timeouts, HTTP errors, rate limits)
            propagate and are NOT cached, so the next pipeline run retries them.
        """
        cached = await self.get(provider, query_type, query, match_version)
        if cached is not None:
            return cached["result"] if cached["status"] == "found" else None

        result, source = fetch_fn(query)
        status = "found" if result is not None else "not_found"
        await self.put(provider, query_type, query, match_version, status, result, source)
        return result
