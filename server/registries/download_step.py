import os
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlparse

import aiohttp

from .snapshot import DownloadRecord, SnapshotStore, slugify_version
from .streaming import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_PROGRESS_INTERVAL,
    DownloadOutcome,
    DownloadProgress,
    human_bytes,
    stream_download,
    work_directory,
)
from ..config import EventType, LocalisationString, StepConfig, StepUserConfig, UserStepConfig
from ..db import get_registry_db_client


@dataclass
class RemoteFile:
    url: str
    version: str
    filename: Optional[str] = None
    headers: Dict[str, str] = field(default_factory=dict)

    def local_name(self) -> str:
        return self.filename or os.path.basename(urlparse(self.url).path) or "download.bin"


class StreamingDownloadStep(StepConfig, metaclass=ABCMeta):
    source: str
    user_agent: Optional[str] = None
    work_dir: Optional[str] = None

    # No total timeout: a multi-gigabyte transfer legitimately runs for a long time, so a
    # stalled socket rather than elapsed time is what should abort it.
    TIMEOUT = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=180)
    CHUNK_SIZE = DEFAULT_CHUNK_SIZE
    PROGRESS_INTERVAL = DEFAULT_PROGRESS_INTERVAL

    STATUS_DOWNLOADED = "downloaded"
    STATUS_NOT_MODIFIED = "not_modified"
    STATUS_UNCHANGED_VERSION = "unchanged_version"

    @abstractmethod
    async def resolve_remote_file(
        self, session: aiohttp.ClientSession, user_config: Optional[UserStepConfig]
    ) -> RemoteFile:
        raise NotImplementedError

    async def run(self, user_config: Optional[UserStepConfig] = None, results: Optional[Dict[str, Any]] = None, **_):
        force = bool((user_config or {}).get("FORCE_DOWNLOAD", False))
        store = SnapshotStore(get_registry_db_client())
        previous = await store.get(self.source)
        has_active = previous is not None and previous.active is not None

        headers = {"User-Agent": self.user_agent} if self.user_agent else {}
        async with aiohttp.ClientSession(timeout=self.TIMEOUT, headers=headers) as session:
            remote = await self.resolve_remote_file(session, user_config)
            yield f'Upstream version for "{self.source}": {remote.version}', EventType.INFO

            if force:
                yield "FORCE_DOWNLOAD is set - ignoring the cached version and ETag", EventType.INFO
            elif has_active and previous.last_download and previous.last_download.version == remote.version:
                yield (
                    f"Version {remote.version} is already active "
                    f"({previous.active.records} records in {previous.active.collection})",
                    EventType.INFO,
                )
                yield self._result(self.STATUS_UNCHANGED_VERSION, remote), EventType.RESULT
                return

            conditional = previous.last_download if (has_active and not force) else None
            destination = work_directory(self.work_dir) / self._destination_name(remote)
            yield f"Downloading {remote.url}", EventType.INFO

            outcome: Optional[DownloadOutcome] = None
            async for event in stream_download(
                session,
                remote.url,
                destination,
                etag=conditional.etag if conditional else None,
                last_modified=conditional.last_modified if conditional else None,
                headers=remote.headers,
                chunk_size=self.CHUNK_SIZE,
                progress_interval=self.PROGRESS_INTERVAL,
            ):
                if isinstance(event, DownloadProgress):
                    yield f"Downloaded {event.describe()}", EventType.INFO
                else:
                    outcome = event

        if outcome is None:
            raise RuntimeError(f'Download of "{self.source}" produced no outcome')

        if outcome.status == DownloadOutcome.NOT_MODIFIED:
            yield f"Upstream reports {self.source} unchanged (HTTP 304)", EventType.INFO
            yield self._result(self.STATUS_NOT_MODIFIED, remote), EventType.RESULT
            return

        record = DownloadRecord(
            version=remote.version,
            url=outcome.url,
            path=str(outcome.path),
            bytes=outcome.bytes,
            sha256=outcome.sha256,
            etag=outcome.etag,
            last_modified=outcome.last_modified,
            downloaded_at=datetime.now(timezone.utc),
        )
        await store.record_download(self.source, record)

        yield f"Downloaded {human_bytes(outcome.bytes)} to {outcome.path}", EventType.INFO
        yield self._result(self.STATUS_DOWNLOADED, remote, record), EventType.RESULT

    def _destination_name(self, remote: RemoteFile) -> str:
        return f"{self.source}-{slugify_version(remote.version)}-{remote.local_name()}"

    def _result(self, status: str, remote: RemoteFile, record: Optional[DownloadRecord] = None) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "source": self.source,
            "status": status,
            "version": remote.version,
            "url": remote.url,
        }
        if record is not None:
            result.update(
                {
                    "path": record.path,
                    "bytes": record.bytes,
                    "sha256": record.sha256,
                    "etag": record.etag,
                    "last_modified": record.last_modified,
                    "downloaded_at": record.downloaded_at.isoformat(),
                }
            )
        return result

    def user_config(self) -> List[StepUserConfig]:
        return [
            StepUserConfig(
                "FORCE_DOWNLOAD",
                LocalisationString("Force download", "Download erzwingen"),
                LocalisationString(
                    "Re-download even when the upstream version is already active.",
                    "Erneut herunterladen, auch wenn die Version bereits aktiv ist.",
                ),
                StepUserConfig.StepUserConfigType.BOOLEAN,
                False,
                required=False,
            ),
        ]

    def dependencies(self) -> Union[List[str], None]:
        return None


class StaticUrlDownloadStep(StreamingDownloadStep, metaclass=ABCMeta):
    url: str
    version: Optional[str] = None

    async def resolve_remote_file(
        self, session: aiohttp.ClientSession, user_config: Optional[UserStepConfig]
    ) -> RemoteFile:
        url = str((user_config or {}).get("SOURCE_URL") or self.url)
        if self.version:
            return RemoteFile(url=url, version=self.version)

        async with session.head(url, allow_redirects=True, ssl=False) as response:
            response.raise_for_status()
            etag = response.headers.get("ETag")
            last_modified = response.headers.get("Last-Modified")

        version = (etag or "").strip('"') or last_modified
        if not version:
            raise RuntimeError(f"{url} exposes neither ETag nor Last-Modified - set an explicit `version`")
        return RemoteFile(url=url, version=version)

    def user_config(self) -> List[StepUserConfig]:
        return [
            *super().user_config(),
            StepUserConfig(
                "SOURCE_URL",
                LocalisationString("Source URL", "Quell-URL"),
                LocalisationString(
                    "Override the download URL, e.g. after the publisher moves the file.",
                    "Download-URL überschreiben, z.B. wenn der Anbieter die Datei verschiebt.",
                ),
                StepUserConfig.StepUserConfigType.STRING,
                None,
                required=False,
            ),
        ]
