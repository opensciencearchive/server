"""IngestRunRepository port — persistence interface for ingest runs."""

from abc import abstractmethod
from datetime import datetime
from typing import Protocol

from osa.domain.ingest.model.ingest_run import IngestRun, IngestRunId, RunUpdate
from osa.domain.shared.failure import FailureKind
from osa.domain.shared.port import Port


class IngestRunRepository(Port, Protocol):
    """Persistence interface for IngestRun aggregates.

    Counter updates (batches_completed, published_count) use atomic SQL
    increments in the concrete implementation to avoid lost updates under
    concurrent ProcessBatch workers.
    """

    @abstractmethod
    async def save(self, ingest_run: IngestRun) -> None:
        """Persist an ingest run (insert or update)."""
        ...

    @abstractmethod
    async def get(self, id: IngestRunId) -> IngestRun | None:
        """Get an ingest run by ID."""
        ...

    @abstractmethod
    async def list(self, *, limit: int = 50) -> list[IngestRun]:
        """List ingest runs, most recently started first."""
        ...

    @abstractmethod
    async def get_running_for_convention(self, convention_id: str) -> IngestRun | None:
        """Get a running ingest run for a convention, if any."""
        ...

    @abstractmethod
    async def increment_batches_ingested(
        self, id: IngestRunId, *, set_ingestion_finished: bool = False
    ) -> RunUpdate:
        """Atomically increment batches_ingested while the run is non-terminal.

        Returns ``Applied`` (with the updated run), or ``RunClosed`` when the run
        is already terminal; raises ``NotFoundError`` if the run is missing.
        """
        ...

    @abstractmethod
    async def mark_batch_ingested(
        self, id: IngestRunId, batch_index: int, *, ingestion_finished: bool
    ) -> RunUpdate:
        """Idempotently record that batch ``batch_index`` was sourced (#160).

        Sets ``batches_ingested = max(batches_ingested, batch_index + 1)`` and
        latches ``ingestion_finished = ingestion_finished OR :flag``, so a
        duplicate delivery (same index) is a harmless no-op and a counter that
        already ran ahead is never rolled back. Guarded to non-terminal runs:
        ``Applied`` (with the run), ``RunClosed`` when already terminal;
        raises ``NotFoundError`` if the run is missing.
        """
        ...

    @abstractmethod
    async def increment_failed(self, id: IngestRunId) -> RunUpdate:
        """Atomically increment batches_failed while the run is non-terminal.

        ``Applied`` (with the run) for completion checking, ``RunClosed`` when
        already terminal; raises ``NotFoundError`` if missing.
        """
        ...

    @abstractmethod
    async def increment_completed(self, id: IngestRunId, published_count: int) -> RunUpdate:
        """Atomically increment batches_completed and published_count, non-terminal only.

        ``Applied`` (with the run) for completion checking, ``RunClosed`` when
        already terminal; raises ``NotFoundError`` if missing.
        """
        ...

    @abstractmethod
    async def abort(
        self,
        id: IngestRunId,
        *,
        reason: str,
        kind: FailureKind,
        completed_at: datetime,
    ) -> RunUpdate:
        """Atomically fail a run with its explanation, if it is not already terminal.

        Sets status=FAILED, failure_reason/failure_kind, ingestion_finished and
        completed_at in one guarded UPDATE so concurrent batch workers can race
        to abort safely. Returns ``Applied`` (with the updated run), or
        ``RunClosed`` if the run was already COMPLETED/FAILED (idempotent no-op).
        """
        ...

    @abstractmethod
    async def record_failure(
        self, id: IngestRunId, *, reason: str, kind: FailureKind | None
    ) -> None:
        """Record why ingestion stopped early, without changing run status.

        Used when the ingester gives up but already-sourced batches may still
        complete the run normally.
        """
        ...
