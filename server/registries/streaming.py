import bz2
import fnmatch
import gzip
import hashlib
import io
import lzma
import os
import tempfile
import zipfile
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, AsyncIterator, Dict, Iterator, List, Optional, TextIO, Union

import aiofiles
import aiohttp

DEFAULT_CHUNK_SIZE = 1 << 20
DEFAULT_PROGRESS_INTERVAL = 64 << 20
WORK_DIR_ENV = "REGISTRY_WORK_DIR"


def work_directory(override: Optional[str] = None) -> Path:
    raw = override or os.environ.get(WORK_DIR_ENV) or os.path.join(tempfile.gettempdir(), "registry-data")
    path = Path(raw)
    path.mkdir(parents=True, exist_ok=True)
    return path


def human_bytes(count: int) -> str:
    size = float(count)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size < 1024 or unit == "TiB":
            return f"{int(size)} B" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TiB"


@dataclass
class DownloadProgress:
    downloaded: int
    total: Optional[int] = None

    @property
    def percentage(self) -> Optional[float]:
        return self.downloaded * 100 / self.total if self.total else None

    def describe(self) -> str:
        if self.total:
            return f"{human_bytes(self.downloaded)} / {human_bytes(self.total)} ({self.percentage:.0f}%)"
        return human_bytes(self.downloaded)


@dataclass
class DownloadOutcome:
    DOWNLOADED = "downloaded"
    NOT_MODIFIED = "not_modified"

    status: str
    url: str
    path: Optional[Path] = None
    bytes: int = 0
    sha256: Optional[str] = None
    etag: Optional[str] = None
    last_modified: Optional[str] = None
    headers: Dict[str, str] = field(default_factory=dict)

    @property
    def was_downloaded(self) -> bool:
        return self.status == self.DOWNLOADED


DownloadEvent = Union[DownloadProgress, DownloadOutcome]


async def stream_download(
    session: aiohttp.ClientSession,
    url: str,
    destination: Path,
    *,
    etag: Optional[str] = None,
    last_modified: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    progress_interval: int = DEFAULT_PROGRESS_INTERVAL,
) -> AsyncIterator[DownloadEvent]:
    # Pass etag/last_modified only when a usable snapshot already exists: a 304 skips the
    # body, which would otherwise leave nothing on disk for the load step to read.
    request_headers: Dict[str, str] = dict(headers or {})
    if etag:
        request_headers["If-None-Match"] = etag
    if last_modified:
        request_headers["If-Modified-Since"] = last_modified

    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_suffix(destination.suffix + ".part")
    digest = hashlib.sha256()
    downloaded = 0

    async with session.get(url, headers=request_headers) as response:
        if response.status == 304:
            yield DownloadOutcome(
                status=DownloadOutcome.NOT_MODIFIED,
                url=url,
                etag=etag,
                last_modified=last_modified,
                headers=dict(response.headers),
            )
            return
        response.raise_for_status()

        total = int(response.headers["Content-Length"]) if "Content-Length" in response.headers else None
        next_report = progress_interval

        async with aiofiles.open(partial, "wb") as handle:
            async for chunk in response.content.iter_chunked(chunk_size):
                await handle.write(chunk)
                digest.update(chunk)
                downloaded += len(chunk)
                if downloaded >= next_report:
                    yield DownloadProgress(downloaded=downloaded, total=total)
                    next_report += progress_interval

        # Read through the case-insensitive multidict before flattening it: aiohttp
        # normalises the key to "Etag", so a plain dict lookup for "ETag" finds nothing
        # and every later run would re-download instead of asking for a 304.
        response_etag = response.headers.get("ETag")
        response_last_modified = response.headers.get("Last-Modified")
        response_headers = dict(response.headers)

    os.replace(partial, destination)
    yield DownloadOutcome(
        status=DownloadOutcome.DOWNLOADED,
        url=url,
        path=destination,
        bytes=downloaded,
        sha256=digest.hexdigest(),
        etag=response_etag,
        last_modified=response_last_modified,
        headers=response_headers,
    )


def list_archive_members(path: Path) -> List[str]:
    if path.suffix.lower() != ".zip":
        return []
    with zipfile.ZipFile(path) as archive:
        return [info.filename for info in archive.infolist() if not info.is_dir()]


def resolve_archive_member(path: Path, pattern: str) -> str:
    members = list_archive_members(path)
    matches = [
        name for name in members if fnmatch.fnmatch(name, pattern) or fnmatch.fnmatch(os.path.basename(name), pattern)
    ]
    if not matches:
        raise FileNotFoundError(f'No member matching "{pattern}" in {path.name}. Members: {sorted(members)[:25]}')
    if len(matches) > 1:
        raise ValueError(f'Pattern "{pattern}" matched several members of {path.name}: {sorted(matches)}')
    return matches[0]


@contextmanager
def binary_stream(path: Path, member: Optional[str] = None) -> Iterator[IO[bytes]]:
    suffix = path.suffix.lower()
    if suffix == ".zip":
        if member is None:
            raise ValueError(f"{path.name} is a zip archive - a member pattern is required")
        with zipfile.ZipFile(path) as archive:
            with archive.open(resolve_archive_member(path, member)) as handle:
                yield handle
    elif suffix == ".gz":
        with gzip.open(path, "rb") as handle:
            yield handle
    elif suffix == ".bz2":
        with bz2.open(path, "rb") as handle:
            yield handle
    elif suffix == ".xz":
        with lzma.open(path, "rb") as handle:
            yield handle
    else:
        with open(path, "rb") as handle:
            yield handle


@contextmanager
def text_stream(
    path: Path,
    member: Optional[str] = None,
    encoding: str = "utf-8",
    errors: str = "strict",
    newline: Optional[str] = "",
) -> Iterator[TextIO]:
    with binary_stream(path, member) as raw:
        yield io.TextIOWrapper(raw, encoding=encoding, errors=errors, newline=newline)


def discard(path: Optional[Path]) -> None:
    if path is None:
        return
    try:
        Path(path).unlink(missing_ok=True)
    except OSError:
        pass
