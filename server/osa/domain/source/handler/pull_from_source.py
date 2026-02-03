"""PullFromSource - handles SourceRequested events."""

from osa.domain.shared.event import EventHandler
from osa.domain.source.event.source_requested import SourceRequested
from osa.domain.source.service import SourceService


class PullFromSource(EventHandler[SourceRequested]):
    """Pulls from a data source and creates depositions.

    This handler delegates to SourceService for all business logic.
    Supports chunked processing with continuation events.
    """

    service: SourceService

    async def handle(self, event: SourceRequested) -> None:
        """Delegate to SourceService to pull records and emit deposition events."""
        await self.service.run_source(
            source_name=event.source_name,
            since=event.since,
            limit=event.limit,
            offset=event.offset,
            chunk_size=event.chunk_size,
            session=event.session,
        )
