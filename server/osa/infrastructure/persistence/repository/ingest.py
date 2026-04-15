"""PostgreSQL implementation of IngestRunRepository."""

import logging

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.ingest.model.ingest_run import IngestRun, IngestStatus
from osa.domain.ingest.port.repository import IngestRunRepository
from osa.infrastructure.persistence.tables import ingest_runs_table

logger = logging.getLogger(__name__)


class PostgresIngestRunRepository(IngestRunRepository):
    """PostgreSQL implementation with atomic counter updates."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(self, ingest_run: IngestRun) -> None:
        """Insert or update an ingest run."""
        values = {
            "id": ingest_run.id,
            "convention_srn": ingest_run.convention_srn,
            "status": ingest_run.status.value,
            "ingestion_finished": ingest_run.ingestion_finished,
            "batches_ingested": ingest_run.batches_ingested,
            "batches_completed": ingest_run.batches_completed,
            "published_count": ingest_run.published_count,
            "batches_failed": ingest_run.batches_failed,
            "batch_size": ingest_run.batch_size,
            "record_limit": ingest_run.limit,
            "started_at": ingest_run.started_at,
            "completed_at": ingest_run.completed_at,
        }
        stmt = (
            insert(ingest_runs_table)
            .values(**values)
            .on_conflict_do_update(
                index_elements=["id"],
                set_=values,
            )
        )
        await self._session.execute(stmt)
        await self._session.flush()

    async def get(self, id: str) -> IngestRun | None:
        stmt = select(ingest_runs_table).where(ingest_runs_table.c.id == id)
        result = await self._session.execute(stmt)
        row = result.mappings().first()
        if row is None:
            return None
        return _row_to_ingest_run(dict(row))

    async def get_running_for_convention(self, convention_srn: str) -> IngestRun | None:
        stmt = (
            select(ingest_runs_table)
            .where(ingest_runs_table.c.convention_srn == convention_srn)
            .where(
                ingest_runs_table.c.status.in_(
                    [IngestStatus.PENDING.value, IngestStatus.RUNNING.value]
                )
            )
            .limit(1)
        )
        result = await self._session.execute(stmt)
        row = result.mappings().first()
        if row is None:
            return None
        return _row_to_ingest_run(dict(row))

    async def increment_batches_ingested(
        self, id: str, *, set_ingestion_finished: bool = False
    ) -> IngestRun:
        """Atomically increment batches_ingested."""
        t = ingest_runs_table
        values = {
            "batches_ingested": t.c.batches_ingested + 1,
        }
        if set_ingestion_finished:
            values["ingestion_finished"] = True

        stmt = update(t).where(t.c.id == id).values(**values).returning(*t.c)
        result = await self._session.execute(stmt)
        await self._session.flush()
        row = result.mappings().first()
        if row is None:
            from osa.domain.shared.error import NotFoundError

            raise NotFoundError(f"Ingest run not found: {id}")
        return _row_to_ingest_run(dict(row))

    async def increment_failed(self, id: str) -> IngestRun:
        """Atomically increment batches_failed."""
        t = ingest_runs_table
        stmt = (
            update(t)
            .where(t.c.id == id)
            .values(batches_failed=t.c.batches_failed + 1)
            .returning(*t.c)
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        row = result.mappings().first()
        if row is None:
            from osa.domain.shared.error import NotFoundError

            raise NotFoundError(f"Ingest run not found: {id}")
        return _row_to_ingest_run(dict(row))

    async def increment_completed(self, id: str, published_count: int) -> IngestRun:
        """Atomically increment batches_completed and published_count."""
        t = ingest_runs_table
        stmt = (
            update(t)
            .where(t.c.id == id)
            .values(
                batches_completed=t.c.batches_completed + 1,
                published_count=t.c.published_count + published_count,
            )
            .returning(*t.c)
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        row = result.mappings().first()
        if row is None:
            from osa.domain.shared.error import NotFoundError

            raise NotFoundError(f"Ingest run not found: {id}")
        return _row_to_ingest_run(dict(row))


def _row_to_ingest_run(row: dict) -> IngestRun:
    return IngestRun(
        id=row["id"],
        convention_srn=row["convention_srn"],
        status=IngestStatus(row["status"]),
        ingestion_finished=row["ingestion_finished"],
        batches_ingested=row["batches_ingested"],
        batches_completed=row["batches_completed"],
        published_count=row["published_count"],
        batches_failed=row.get("batches_failed", 0),
        batch_size=row["batch_size"],
        limit=row.get("record_limit"),
        started_at=row["started_at"],
        completed_at=row.get("completed_at"),
    )
