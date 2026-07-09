"""Integration tests for IngestRunRepository failure surfacing (#152).

Covers the queryable failure columns against real PostgreSQL: `record_failure`
round-trips reason/kind, and it must NOT clobber a reason already set (so a
later per-batch give-up can't overwrite an earlier run abort).
"""

from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.ingest.model.ingest_run import (
    Applied,
    IngestRun,
    IngestRunId,
    IngestStatus,
    RunClosed,
)
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.failure import FailureKind
from osa.infrastructure.persistence.repository.ingest import PostgresIngestRunRepository


def _make_run(run_id: str = "ing-1", status: IngestStatus = IngestStatus.RUNNING) -> IngestRun:
    return IngestRun(
        id=IngestRunId(run_id),
        convention_id="test-conv",
        status=status,
        batch_size=100,
        batches_ingested=1,
        started_at=datetime.now(UTC),
    )


@pytest.mark.asyncio
class TestFailureSurfacing:
    async def test_record_failure_round_trips_reason_and_kind(self, pg_session: AsyncSession):
        repo = PostgresIngestRunRepository(pg_session)
        await repo.save(_make_run("ing-rt"))

        await repo.record_failure("ing-rt", reason="source exited 3", kind=FailureKind.UPSTREAM)

        fetched = await repo.get(IngestRunId("ing-rt"))
        assert fetched is not None
        assert fetched.failure_reason == "source exited 3"
        assert fetched.failure_kind is FailureKind.UPSTREAM

    async def test_record_failure_allows_null_kind(self, pg_session: AsyncSession):
        repo = PostgresIngestRunRepository(pg_session)
        await repo.save(_make_run("ing-null"))

        await repo.record_failure("ing-null", reason="retries exhausted", kind=None)

        fetched = await repo.get(IngestRunId("ing-null"))
        assert fetched is not None
        assert fetched.failure_reason == "retries exhausted"
        assert fetched.failure_kind is None

    async def test_record_failure_does_not_clobber_an_existing_reason(
        self, pg_session: AsyncSession
    ):
        """An abort sets the reason first; a later batch give-up must not overwrite it."""
        repo = PostgresIngestRunRepository(pg_session)
        await repo.save(_make_run("ing-clobber"))

        aborted = await repo.abort(
            "ing-clobber",
            reason="Image pull failed: 401",
            kind=FailureKind.IMAGE_PULL,
            completed_at=datetime.now(UTC),
        )
        assert isinstance(aborted, Applied)

        # A batch that was mid-retry exhausts after the abort and records its own reason.
        await repo.record_failure(
            "ing-clobber", reason="batch 5: hook retries exhausted", kind=None
        )

        fetched = await repo.get(IngestRunId("ing-clobber"))
        assert fetched is not None
        # The more-significant abort reason survives.
        assert fetched.failure_reason == "Image pull failed: 401"
        assert fetched.failure_kind is FailureKind.IMAGE_PULL
        assert fetched.status is IngestStatus.FAILED


@pytest.mark.asyncio
class TestTerminalRunGuard:
    """Counter increments are no-ops once a run is terminal (#152).

    A stale in-flight batch that exhausts after abort_run must not accumulate
    failed/completed counts on the closed run — the atomic UPDATE is guarded on
    non-terminal status, mirroring `abort`.
    """

    async def test_increment_failed_is_noop_on_terminal_run(self, pg_session: AsyncSession):
        repo = PostgresIngestRunRepository(pg_session)
        await repo.save(_make_run("ing-tf"))
        await repo.abort(
            "ing-tf",
            reason="bad image",
            kind=FailureKind.IMAGE_PULL,
            completed_at=datetime.now(UTC),
        )

        result = await repo.increment_failed("ing-tf")

        assert isinstance(result, RunClosed)  # guarded — the closed run is not mutated
        fetched = await repo.get(IngestRunId("ing-tf"))
        assert fetched is not None
        assert fetched.batches_failed == 0
        assert fetched.status is IngestStatus.FAILED

    async def test_increment_completed_is_noop_on_terminal_run(self, pg_session: AsyncSession):
        repo = PostgresIngestRunRepository(pg_session)
        await repo.save(_make_run("ing-tc"))
        await repo.abort(
            "ing-tc", reason="rbac", kind=FailureKind.RBAC, completed_at=datetime.now(UTC)
        )

        result = await repo.increment_completed("ing-tc", published_count=5)

        assert isinstance(result, RunClosed)
        fetched = await repo.get(IngestRunId("ing-tc"))
        assert fetched is not None
        assert fetched.batches_completed == 0
        assert fetched.published_count == 0

    async def test_increment_still_works_on_a_running_run(self, pg_session: AsyncSession):
        """Sanity: the guard doesn't break the normal (non-terminal) path."""
        repo = PostgresIngestRunRepository(pg_session)
        await repo.save(_make_run("ing-live"))

        result = await repo.increment_failed("ing-live")

        assert isinstance(result, Applied)
        assert result.run.batches_failed == 1

    async def test_increment_on_missing_run_raises(self, pg_session: AsyncSession):
        """A genuinely-missing run is exceptional — not a RunClosed no-op."""
        repo = PostgresIngestRunRepository(pg_session)
        with pytest.raises(NotFoundError):
            await repo.increment_failed("does-not-exist")
