"""Ingest domain events — payloads carry path references, not inline data (AD-1)."""

from osa.domain.shared.event import Event, EventId


class IngestStarted(Event):
    """Emitted when an ingest run is created. Triggers first ingester pull."""

    id: EventId
    ingest_run_srn: str
    convention_srn: str
    batch_size: int


class IngesterBatchReady(Event):
    """Emitted when an ingester container produces a batch of records.

    Batch data is on disk at the path derived from {ingest_run_srn, batch_index}.
    """

    id: EventId
    ingest_run_srn: str
    batch_index: int
    has_more: bool


class HookBatchCompleted(Event):
    """Emitted when hook processing completes for a batch.

    Outcomes (features/rejections/errors) are on disk at the batch output path.
    """

    id: EventId
    ingest_run_srn: str
    batch_index: int


class IngestBatchPublished(Event):
    """Emitted when records from a batch are bulk-published.

    Triggers InsertBatchFeatures for feature insertion.
    Batch-level event — NOT per-record (AD-3).
    """

    id: EventId
    ingest_run_srn: str
    convention_srn: str
    batch_index: int
    published_srns: list[str]
    published_count: int
    expected_features: list[str]


class IngestCompleted(Event):
    """Emitted when all batches are processed and the ingest run is complete."""

    id: EventId
    ingest_run_srn: str
    total_published: int
