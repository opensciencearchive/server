"""KeywordIndexHandler - processes IndexRecord events for keyword backends."""

import logging
from typing import ClassVar

from osa.domain.index.event.index_record import IndexRecord
from osa.domain.index.model.registry import IndexRegistry
from osa.domain.shared.event import EventHandler

logger = logging.getLogger(__name__)


class KeywordIndexHandler(EventHandler[IndexRecord]):
    """Processes IndexRecord events for the keyword backend.

    Claims events with routing_key="keyword" and processes them immediately
    (batch_size=1) since keyword indexing doesn't benefit from batching.
    """

    __routing_key__: ClassVar[str | None] = "keyword"
    __batch_size__: ClassVar[int] = 1

    indexes: IndexRegistry

    async def handle(self, event: IndexRecord) -> None:
        """Process a single IndexRecord event."""
        backend = self.indexes.get("keyword")
        if backend is None:
            logger.warning("Keyword backend not available, skipping event")
            return

        record = (str(event.record_srn), event.metadata)

        await backend.ingest_batch([record])

        logger.debug(f"KeywordIndexHandler: indexed event {event.id}")
