"""SourceListener - handles SourceRequested events."""

from osa.domain.shared.event import EventListener
from osa.domain.source.event.source_requested import SourceRequested
from osa.domain.source.service import SourceService


class PullFromSource(EventListener[SourceRequested]):
    """Pulls from a data source and creates depositions.

    This listener delegates to SourceService for all business logic.
    """

    service: SourceService

    async def handle(self, event: SourceRequested) -> None:
        """Delegate to SourceService to pull records and emit deposition events."""
        await self.service.run_source(
            source_name=event.source_name,
            since=event.since,
            limit=event.limit,
        )
