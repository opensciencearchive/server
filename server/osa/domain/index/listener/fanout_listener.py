"""FanOutToIndexBackends - creates per-backend IndexRecord events from RecordPublished."""

from uuid import uuid4

from osa.domain.index.event.index_record import IndexRecord
from osa.domain.index.model.registry import IndexRegistry
from osa.domain.record.event.record_published import RecordPublished
from osa.domain.shared.event import EventId, EventListener
from osa.domain.shared.outbox import Outbox


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
        for backend_name in self.indexes:
            index_event = IndexRecord(
                id=EventId(uuid4()),
                backend_name=backend_name,
                record_srn=event.record_srn,
                metadata=event.metadata,
            )
            await self.outbox.append(index_event)
