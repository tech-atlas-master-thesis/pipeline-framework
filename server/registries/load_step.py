import asyncio
from abc import ABCMeta, abstractmethod
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Union

from pymongo import IndexModel

from .snapshot import SnapshotStore
from .streaming import discard
from ..config import (
    EventType,
    LocalisationString,
    LocalisationStringType,
    StepConfig,
    StepUserConfig,
    UserStepConfig,
)
from ..db import get_registry_db_client


class SnapshotLoadStep(StepConfig, metaclass=ABCMeta):
    source: str
    download_step: type[StepConfig]

    BATCH_SIZE = 2000
    KEEP_SNAPSHOTS = 2
    PROGRESS_INTERVAL = 50_000
    delete_archive = True

    @abstractmethod
    def iter_documents(self, path: Path) -> Iterator[Dict[str, Any]]:
        raise NotImplementedError

    def indexes(self) -> List[IndexModel]:
        return []

    async def run(self, user_config: Optional[UserStepConfig] = None, results: Optional[Dict[str, Any]] = None, **_):
        download = (results or {}).get(self.download_step.name())
        if download is None:
            raise FileNotFoundError(f'No result from download step "{self.download_step.name()}"')

        if download.get("status") != "downloaded":
            yield f"Nothing new for {self.source} ({download.get('status')})", EventType.INFO
            yield {"source": self.source, "status": "skipped", "version": download.get("version")}, EventType.RESULT
            return

        path = Path(download["path"])
        if not path.exists():
            raise FileNotFoundError(f"Downloaded archive {path} is gone - re-run with FORCE_DOWNLOAD")

        version = download["version"]
        db = get_registry_db_client()
        store = SnapshotStore(db)
        target_name = store.collection_name(self.source, version)

        if target_name in await db.list_collection_names():
            yield f"Collection {target_name} already exists - dropping it before reloading", EventType.WARNING
            await db.drop_collection(target_name)

        target = db[target_name]
        yield f"Loading {path.name} into {target_name}", EventType.INFO

        total = 0
        next_report = self.PROGRESS_INTERVAL
        batch: List[Dict[str, Any]] = []

        for document in self.iter_documents(path):
            batch.append(document)
            if len(batch) >= self.BATCH_SIZE:
                await target.insert_many(batch, ordered=False)
                total += len(batch)
                batch = []
                await asyncio.sleep(0)
                if total >= next_report:
                    yield f"Inserted {total:,} documents", EventType.INFO
                    next_report += self.PROGRESS_INTERVAL

        if batch:
            await target.insert_many(batch, ordered=False)
            total += len(batch)

        if total == 0:
            await db.drop_collection(target_name)
            raise ValueError(
                f"Parsing {path.name} produced no documents for {self.source}. "
                f"Check the parser against the current file layout."
            )

        indexes = self.indexes()
        if indexes:
            await target.create_indexes(indexes)
            yield f"Built {len(indexes)} index(es) on {target_name}", EventType.INFO

        previous = await store.get(self.source)
        previous_name = previous.active.collection if previous and previous.active else None

        await store.activate(self.source, target_name, version, total)
        yield (
            f"Activated {self.source} version {version}: {total:,} documents in {target_name}"
            + (f" (was {previous_name})" if previous_name else ""),
            EventType.INFO,
        )

        dropped = await store.prune(self.source, keep=self.KEEP_SNAPSHOTS)
        if dropped:
            yield f"Pruned old snapshot(s): {', '.join(dropped)}", EventType.INFO

        if self.delete_archive:
            discard(path)

        yield {
            "source": self.source,
            "status": "loaded",
            "version": version,
            "collection": target_name,
            "records": total,
            "replaced": previous_name,
            "pruned": dropped,
        }, EventType.RESULT

    def user_config(self) -> List[StepUserConfig]:
        return []

    def dependencies(self) -> Union[List[str], None]:
        return [self.download_step.name()]


class SnapshotSummaryStep(StepConfig):
    def __init__(self, depends_on: Optional[List[type[StepConfig]]] = None):
        self._depends_on = list(depends_on or [])

    async def run(self, user_config: Optional[UserStepConfig] = None, results: Optional[Dict[str, Any]] = None, **_):
        summary = await SnapshotStore(get_registry_db_client()).describe()
        if not summary:
            yield "No registry snapshots are active yet", EventType.WARNING
        for source, state in sorted(summary.items()):
            yield (
                f"{source}: version {state['version']} - {state['records']:,} records in {state['collection']}",
                EventType.INFO,
            )
        yield summary, EventType.RESULT

    def user_config(self) -> List[StepUserConfig]:
        return []

    @staticmethod
    def name() -> str:
        return "registry_summary"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Registry data summary", "Übersicht Registerdaten")

    @staticmethod
    def description() -> Optional[LocalisationStringType]:
        return LocalisationString(
            "Which snapshot each registry currently resolves to.",
            "Auf welchen Snapshot jedes Register aktuell verweist.",
        )

    def dependencies(self) -> Union[List[str], None]:
        return [step.name() for step in self._depends_on] or None
