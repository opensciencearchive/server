"""IngestRunRepository port — persistence interface for ingest runs."""

from abc import abstractmethod
from typing import Protocol

from osa.domain.ingest.model.ingest_run import IngestRun
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
    async def get(self, srn: str) -> IngestRun | None:
        """Get an ingest run by SRN."""
        ...

    @abstractmethod
    async def get_running_for_convention(self, convention_srn: str) -> IngestRun | None:
        """Get a running ingest run for a convention, if any."""
        ...

    @abstractmethod
    async def increment_batches_sourced(
        self, srn: str, *, set_source_finished: bool = False
    ) -> IngestRun:
        """Atomically increment batches_sourced and optionally set source_finished.

        Returns the updated IngestRun with DB-authoritative counter values.
        """
        ...

    @abstractmethod
    async def increment_completed(self, srn: str, published_count: int) -> IngestRun:
        """Atomically increment batches_completed and published_count.

        Returns the updated IngestRun with DB-authoritative counter values
        for completion condition checking.
        """
        ...
