"""Ingest domain events — payloads carry path references, not inline data (AD-1)."""

from osa.domain.ingest.model.ingest_run import IngestRunId
from osa.domain.shared.event import Event, EventId


class IngestRunStarted(Event):
    """Emitted once when an ingest run is created. Observability/audit only."""

    id: EventId
    ingest_run_id: IngestRunId
    convention_srn: str
    batch_size: int


class NextBatchRequested(Event):
    """Emitted to trigger the next ingester batch pull.

    Emitted by StartIngest (first batch) and by RunIngester (continuation).
    RunIngester is the only handler that listens to this event.
    """

    id: EventId
    ingest_run_id: IngestRunId
    convention_srn: str
    batch_size: int


class IngesterBatchReady(Event):
    """Emitted when an ingester container produces a batch of records.

    Batch data is on disk at the path derived from {ingest_run_id, batch_index}.
    """

    id: EventId
    ingest_run_id: IngestRunId
    batch_index: int
    has_more: bool


class HookBatchCompleted(Event):
    """Emitted when hook processing completes for a batch.

    Outcomes (features/rejections/errors) are on disk at the batch output path.
    """

    id: EventId
    ingest_run_id: IngestRunId
    batch_index: int


class IngestBatchPublished(Event):
    """Emitted when records from a batch are bulk-published.

    Triggers InsertBatchFeatures for feature insertion.
    Batch-level event — NOT per-record (AD-3).
    """

    id: EventId
    ingest_run_id: IngestRunId
    convention_srn: str
    batch_index: int
    published_srns: list[str]
    published_count: int
    expected_features: list[str]
    upstream_to_record_srn: dict[str, str]  # upstream source ID → published record SRN


class IngestCompleted(Event):
    """Emitted when all batches are processed and the ingest run is complete."""

    id: EventId
    ingest_run_id: IngestRunId
    total_published: int
