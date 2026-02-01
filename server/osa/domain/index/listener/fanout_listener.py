"""FanOutToIndexBackends - creates per-backend IndexRecord events from RecordPublished."""

import logging
from uuid import uuid4

from osa.domain.index.event.index_record import IndexRecord
from osa.domain.index.model.registry import IndexRegistry
from osa.domain.record.event.record_published import RecordPublished
from osa.domain.shared.event import EventId, EventListener
from osa.domain.shared.outbox import Outbox

logger = logging.getLogger(__name__)


class FanOutToIndexBackends(EventListener[RecordPublished]):
    """Creates per-backend IndexRecord events from RecordPublished.

    When a record is published, this listener creates one IndexRecord event
    per registered backend. Each IndexRecord is stored in the outbox,
    enabling independent retry and failure isolation per backend.

    This replaces the previous pattern where a single RecordPublished event
    triggered immediate indexing to all backends in a single transaction.
    """

    indexes: IndexRegistry
    outbox: Outbox

    async def handle(self, event: RecordPublished) -> None:
        """Create IndexRecord events for each registered backend."""
        backend_names = list(self.indexes)
        logger.debug(f"FanOut: {event.record_srn} -> {len(backend_names)} backends")

        for backend_name in backend_names:
            index_event = IndexRecord(
                id=EventId(uuid4()),
                backend_name=backend_name,
                record_srn=event.record_srn,
                metadata=event.metadata,
            )
            await self.outbox.append(index_event)
