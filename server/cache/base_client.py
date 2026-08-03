from abc import ABC, abstractmethod
from typing import Optional

from .cache import EnrichmentCache


class BaseEnrichmentClient(ABC):
    provider: str  # e.g. "ror"
    query_type: str  # e.g. "org_name"
    match_version: int = 1  # bump if _fetch's matching logic changes

    def __init__(self, cache: EnrichmentCache):
        self.cache = cache

    def lookup(self, query: str) -> Optional[dict]:
        return self.cache.lookup(
            self.provider,
            self.query_type,
            query,
            self._fetch,
            self.match_version,
        )

    @abstractmethod
    def _fetch(self, query: str) -> tuple:
        """Call the external API for `query`.

        Return (result, source_metadata):
          - result: dict describing the match, or None if the API genuinely
            reports no match (this None gets cached).
          - source_metadata: dict for auditability (request URL, raw ID, etc.),
            may be None.

        Let exceptions (timeouts, HTTP errors, rate limiting) propagate -
        do not catch and convert them to a "not found" result, or you will
        permanently cache a transient failure.
        """
        raise NotImplementedError
