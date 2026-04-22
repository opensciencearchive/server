"""InsertBatchMetadata — bulk metadata projection for ingest batches.

Mirrors :class:`InsertBatchFeatures` — listens to ``IngestBatchPublished``
rather than per-record ``RecordPublished``, because the bulk ingest pipeline
emits one batch-level event instead of N per-record ones (AD-3).
"""

from __future__ import annotations

from osa.domain.ingest.event.events import IngestBatchPublished
from osa.domain.metadata.service.metadata import MetadataService
from osa.domain.record.port.repository import RecordRepository
from osa.domain.shared.event import EventHandler
from osa.domain.shared.model.srn import RecordSRN
from osa.infrastructure.logging import get_logger

log = get_logger(__name__)


class InsertBatchMetadata(EventHandler[IngestBatchPublished]):
    """Project each newly-published record's metadata into its typed table."""

    metadata_service: MetadataService
    record_repo: RecordRepository

    async def handle(self, event: IngestBatchPublished) -> None:
        if not event.published_srns:
            return

        inserted = 0
        for srn_str in event.published_srns:
            srn = RecordSRN.parse(srn_str)
            record = await self.record_repo.get(srn)
            if record is None:
                # Record was published in this batch but we can't find it —
                # would indicate the same UOW is reading stale state. Skip.
                continue
            await self.metadata_service.insert(
                schema_id=record.schema_id,
                record_srn=record.srn,
                values=record.metadata,
            )
            inserted += 1

        short_id = event.ingest_run_id[:8]
        log.info(
            "[{short_id}] batch {batch_index}: inserted {inserted} metadata rows",
            short_id=short_id,
            batch_index=event.batch_index,
            inserted=inserted,
            ingest_run_id=event.ingest_run_id,
        )
