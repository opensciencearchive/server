"""IngestListener - handles IngestRequested events."""

from osa.domain.ingest.event.ingest_requested import IngestRequested
from osa.domain.ingest.service import IngestService
from osa.domain.shared.event import EventListener


class IngestFromUpstream(EventListener[IngestRequested]):
    """Pulls from upstream source and creates depositions.

    This listener delegates to IngestService for all business logic.
    """

    service: IngestService

    async def handle(self, event: IngestRequested) -> None:
        """Delegate to IngestService to pull records and emit deposition events."""
        await self.service.run_ingest(
            ingestor_name=event.ingestor_name,
            since=event.since,
            limit=event.limit,
        )
