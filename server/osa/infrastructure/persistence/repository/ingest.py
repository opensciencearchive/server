"""PostgreSQL implementation of IngestRunRepository."""

import logging
from datetime import datetime

from sqlalchemy import RowMapping, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.ingest.model.ingest_run import (
    Applied,
    IngestRun,
    IngestStatus,
    RunClosed,
    RunUpdate,
)
from osa.domain.ingest.port.repository import IngestRunRepository
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.failure import FailureKind
from osa.infrastructure.persistence.tables import ingest_runs_table

logger = logging.getLogger(__name__)

# Counter/state mutations only apply while a run is non-terminal — a stale batch
# that finishes after abort_run/completion must not mutate the closed run (#152).
_NON_TERMINAL = (IngestStatus.PENDING.value, IngestStatus.RUNNING.value)


class PostgresIngestRunRepository(IngestRunRepository):
    """PostgreSQL implementation with atomic counter updates."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(self, ingest_run: IngestRun) -> None:
        """Insert or update an ingest run."""
        values = {
            "id": ingest_run.id,
            "convention_id": ingest_run.convention_id,
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
            "failure_reason": ingest_run.failure_reason,
            "failure_kind": ingest_run.failure_kind.value if ingest_run.failure_kind else None,
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

    async def get_running_for_convention(self, convention_id: str) -> IngestRun | None:
        stmt = (
            select(ingest_runs_table)
            .where(ingest_runs_table.c.convention_id == convention_id)
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

    async def _applied_or_closed(self, row: RowMapping | None, id: str) -> RunUpdate:
        """Interpret a status-guarded UPDATE's RETURNING row (#152).

        A row means the mutation landed → :class:`Applied`. Zero rows means the
        guard rejected it: distinguish an already-terminal run (an expected
        concurrent no-op → :class:`RunClosed`) from a genuinely-missing run (a
        bug → ``NotFoundError``). The classifying read runs only on the miss and
        is race-stable — runs are neither created-with-this-id nor deleted
        concurrently.
        """
        if row is not None:
            return Applied(run=_row_to_ingest_run(dict(row)))
        if await self.get(id) is None:
            raise NotFoundError(f"Ingest run not found: {id}")
        return RunClosed()

    async def increment_batches_ingested(
        self, id: str, *, set_ingestion_finished: bool = False
    ) -> RunUpdate:
        """Atomically increment batches_ingested while the run is non-terminal (#152)."""
        t = ingest_runs_table
        values = {
            "batches_ingested": t.c.batches_ingested + 1,
        }
        if set_ingestion_finished:
            values["ingestion_finished"] = True

        stmt = (
            update(t)
            .where(t.c.id == id)
            .where(t.c.status.in_(_NON_TERMINAL))
            .values(**values)
            .returning(*t.c)
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return await self._applied_or_closed(result.mappings().first(), id)

    async def increment_failed(self, id: str) -> RunUpdate:
        """Atomically increment batches_failed while the run is non-terminal (#152).

        A stale batch that exhausts after abort must not accumulate failures on
        the closed run — the guard makes that a :class:`RunClosed` no-op.
        """
        t = ingest_runs_table
        stmt = (
            update(t)
            .where(t.c.id == id)
            .where(t.c.status.in_(_NON_TERMINAL))
            .values(batches_failed=t.c.batches_failed + 1)
            .returning(*t.c)
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return await self._applied_or_closed(result.mappings().first(), id)

    async def increment_completed(self, id: str, published_count: int) -> RunUpdate:
        """Atomically increment batches_completed and published_count, non-terminal only (#152)."""
        t = ingest_runs_table
        stmt = (
            update(t)
            .where(t.c.id == id)
            .where(t.c.status.in_(_NON_TERMINAL))
            .values(
                batches_completed=t.c.batches_completed + 1,
                published_count=t.c.published_count + published_count,
            )
            .returning(*t.c)
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return await self._applied_or_closed(result.mappings().first(), id)

    async def abort(
        self,
        id: str,
        *,
        reason: str,
        kind: FailureKind,
        completed_at: datetime,
    ) -> RunUpdate:
        """Fail a non-terminal run with its explanation, in one guarded UPDATE (#152).

        :class:`RunClosed` when another worker terminalized it first (idempotent).
        """
        t = ingest_runs_table
        stmt = (
            update(t)
            .where(t.c.id == id)
            .where(t.c.status.in_(_NON_TERMINAL))
            .values(
                status=IngestStatus.FAILED.value,
                failure_reason=reason,
                failure_kind=kind.value,
                ingestion_finished=True,
                completed_at=completed_at,
            )
            .returning(*t.c)
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return await self._applied_or_closed(result.mappings().first(), id)

    async def record_failure(self, id: str, *, reason: str, kind: FailureKind | None) -> None:
        """Record why a run failed / ingestion stopped early, without touching status.

        First-writer-wins: the ``failure_reason IS NULL`` guard means a run
        abort (which sets the reason via :meth:`abort`) is never clobbered by a
        later per-batch give-up that races in behind it (#152).
        """
        t = ingest_runs_table
        stmt = (
            update(t)
            .where(t.c.id == id)
            .where(t.c.failure_reason.is_(None))
            .values(
                failure_reason=reason,
                failure_kind=kind.value if kind else None,
            )
        )
        await self._session.execute(stmt)
        await self._session.flush()


def _row_to_ingest_run(row: dict) -> IngestRun:
    raw_kind = row.get("failure_kind")
    return IngestRun(
        id=row["id"],
        convention_id=row["convention_id"],
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
        failure_reason=row.get("failure_reason"),
        failure_kind=FailureKind(raw_kind) if raw_kind else None,
    )
