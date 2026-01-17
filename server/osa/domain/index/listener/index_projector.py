"""IndexProjector - indexes published records into storage backends."""

from osa.domain.index.service import IndexService
from osa.domain.record.event.record_published import RecordPublished
from osa.domain.shared.event import EventListener


class ProjectNewRecordToIndexes(EventListener[RecordPublished]):
    """Projects published records into index backends.

    This listener delegates to IndexService for all business logic.
    """

    service: IndexService

    async def handle(self, event: RecordPublished) -> None:
        """Delegate to IndexService to index the record."""
        await self.service.index_record(
            record_srn=event.record_srn,
            metadata=event.metadata,
        )
