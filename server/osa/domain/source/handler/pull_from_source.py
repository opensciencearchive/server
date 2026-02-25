"""PullFromSource - handles SourceRequested events."""

from osa.domain.shared.event import EventHandler
from osa.domain.source.event.source_requested import SourceRequested
from osa.domain.source.service import SourceService


class PullFromSource(EventHandler[SourceRequested]):
    """Runs a source container and creates depositions from its output.

    This handler delegates to SourceService for all business logic.
    Supports chunked processing with continuation events.
    """

    service: SourceService

    async def handle(self, event: SourceRequested) -> None:
        """Delegate to SourceService to run source container and create depositions."""
        await self.service.run_source(
            convention_srn=event.convention_srn,
            since=event.since,
            limit=event.limit,
            offset=event.offset,
            chunk_size=event.chunk_size,
            session=event.session,
        )
