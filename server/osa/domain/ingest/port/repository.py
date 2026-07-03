"""IngestRunRepository port — persistence interface for ingest runs."""

from abc import abstractmethod
from datetime import datetime
from typing import Protocol

from osa.domain.ingest.model.ingest_run import IngestRun, IngestRunId
from osa.domain.shared.failure import FailureKind
from osa.domain.shared.port import Port


class IngestRunRepository(Port, Protocol):
    """Persistence interface for IngestRun aggregates.

    Counter updates (batches_completed, published_count) use atomic SQL
    increments in the concrete implementation to avoid lost updates under
    concurrent PublishBatch workers.
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
    async def get_running_for_convention(self, convention_id: str) -> IngestRun | None:
        """Get a running ingest run for a convention, if any."""
        ...

    @abstractmethod
    async def increment_batches_ingested(
        self, id: IngestRunId, *, set_ingestion_finished: bool = False
    ) -> IngestRun:
        """Atomically increment batches_ingested and optionally set ingestion_finished.

        Returns the updated IngestRun with DB-authoritative counter values.
        """
        ...

    @abstractmethod
    async def increment_failed(self, id: IngestRunId) -> IngestRun:
        """Atomically increment batches_failed.

        Returns the updated IngestRun for completion condition checking.
        """
        ...

    @abstractmethod
    async def increment_completed(self, id: IngestRunId, published_count: int) -> IngestRun:
        """Atomically increment batches_completed and published_count.

        Returns the updated IngestRun with DB-authoritative counter values
        for completion condition checking.
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
    ) -> IngestRun | None:
        """Atomically fail a run with its explanation, if it is not already terminal.

        Sets status=FAILED, failure_reason/failure_kind, ingestion_finished and
        completed_at in one guarded UPDATE so concurrent batch workers can race
        to abort safely. Returns the updated run, or None if the run was
        already COMPLETED/FAILED (idempotent no-op).
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
