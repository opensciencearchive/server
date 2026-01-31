"""FlushListener - flushes index backends when source run's final chunk completes."""

from osa.domain.index.service import IndexService
from osa.domain.shared.event import EventListener
from osa.domain.source.event.source_run_completed import SourceRunCompleted


class FlushIndexesOnSourceComplete(EventListener[SourceRunCompleted]):
    """Flushes index backends when a source run's final chunk completes.

    This ensures all buffered records are persisted before the source run
    is considered fully complete, enabling downstream consumers to see
    all indexed records.
    """

    service: IndexService

    async def handle(self, event: SourceRunCompleted) -> None:
        """Flush all index backends if this is the final chunk."""
        if event.is_final_chunk:
            await self.service.flush_all()
