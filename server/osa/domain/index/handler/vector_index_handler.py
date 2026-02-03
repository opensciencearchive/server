"""VectorIndexHandler - processes IndexRecord events for vector backends."""

import logging
from typing import ClassVar

from osa.domain.index.event.index_record import IndexRecord
from osa.domain.index.model.registry import IndexRegistry
from osa.domain.shared.error import SkippedEvents
from osa.domain.shared.event import EventHandler

logger = logging.getLogger(__name__)


class VectorIndexHandler(EventHandler[IndexRecord]):
    """Processes IndexRecord events for the vector backend.

    Claims events with routing_key="vector" and processes them in batches
    for efficient embedding generation.
    """

    __routing_key__: ClassVar[str | None] = "vector"
    __batch_size__: ClassVar[int] = 100
    __batch_timeout__: ClassVar[float] = 5.0

    indexes: IndexRegistry

    async def handle_batch(self, events: list[IndexRecord]) -> None:
        """Process a batch of IndexRecord events.

        Converts events to records and calls ingest_batch on the backend.
        """
        if not events:
            return

        backend = self.indexes.get("vector")
        if backend is None:
            raise SkippedEvents(
                event_ids=[e.id for e in events],
                reason="Vector backend not available",
            )

        # Prepare records for batch ingestion
        records = [(str(e.record_srn), e.metadata) for e in events]

        logger.debug(f"VectorIndexHandler: ingesting {len(records)} records to backend")

        await backend.ingest_batch(records)

        logger.debug(f"VectorIndexHandler: ingested {len(events)} records")
