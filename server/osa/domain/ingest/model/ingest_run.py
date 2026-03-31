"""IngestRun aggregate — lean summary tracking a bulk ingestion execution."""

from datetime import datetime
from enum import StrEnum

from osa.domain.shared.error import InvalidStateError
from osa.domain.shared.model.aggregate import Aggregate


class IngestStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


_VALID_TRANSITIONS: dict[IngestStatus, set[IngestStatus]] = {
    IngestStatus.PENDING: {IngestStatus.RUNNING, IngestStatus.FAILED},
    IngestStatus.RUNNING: {IngestStatus.COMPLETED, IngestStatus.FAILED},
    IngestStatus.COMPLETED: set(),
    IngestStatus.FAILED: set(),
}


class IngestRun(Aggregate):
    """Lean summary aggregate tracking a bulk ingestion execution.

    No per-record data — batch output directories on disk are the audit trail.
    Counter updates use atomic SQL increments in the repository.
    """

    srn: str
    convention_srn: str
    status: IngestStatus = IngestStatus.PENDING
    ingestion_finished: bool = False
    batches_ingested: int = 0
    batches_completed: int = 0
    published_count: int = 0
    batch_size: int = 1000
    limit: int | None = None  # Max total records (None = unlimited)
    started_at: datetime
    completed_at: datetime | None = None

    def transition_to(self, new_status: IngestStatus) -> None:
        """Transition to a new status, enforcing valid transitions."""
        if new_status not in _VALID_TRANSITIONS[self.status]:
            raise InvalidStateError(f"Cannot transition from {self.status} to {new_status}")
        self.status = new_status

    def mark_running(self) -> None:
        self.transition_to(IngestStatus.RUNNING)

    def mark_failed(self, completed_at: datetime) -> None:
        self.transition_to(IngestStatus.FAILED)
        self.completed_at = completed_at

    def mark_ingestion_finished(self) -> None:
        self.ingestion_finished = True

    def increment_batches_ingested(self) -> None:
        self.batches_ingested += 1

    def record_batch_completed(self, published_count: int) -> None:
        """Record a completed batch with its published count.

        In production, counter updates use atomic SQL increments —
        this method is for in-memory aggregate state only.
        """
        self.batches_completed += 1
        self.published_count += published_count

    @property
    def is_complete(self) -> bool:
        """Check the completion condition: all sourced batches are completed."""
        return self.ingestion_finished and self.batches_ingested == self.batches_completed

    def check_completion(self, completed_at: datetime) -> bool:
        """Check completion condition and transition if met.

        Returns True if the ingest run is now complete.
        """
        if self.is_complete and self.status == IngestStatus.RUNNING:
            self.transition_to(IngestStatus.COMPLETED)
            self.completed_at = completed_at
            return True
        return False
