"""PullFromSource - handles SourceRequested events."""

from osa.domain.deposition.service.convention import ConventionService
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.event import EventHandler
from osa.domain.source.event.source_requested import SourceRequested
from osa.domain.source.service import SourceService


class PullFromSource(EventHandler[SourceRequested]):
    """Runs a source container and emits per-record events.

    Looks up the convention to get the SourceDefinition, then
    delegates to SourceService for container execution.
    """

    service: SourceService
    convention_service: ConventionService

    async def handle(self, event: SourceRequested) -> None:
        """Look up convention, extract source definition, and run source."""
        convention = await self.convention_service.get_convention(event.convention_srn)
        if convention.source is None:
            raise NotFoundError(f"No source defined for convention {event.convention_srn}")

        await self.service.run_source(
            convention_srn=event.convention_srn,
            source=convention.source,
            since=event.since,
            limit=event.limit,
            offset=event.offset,
            chunk_size=event.chunk_size,
            session=event.session,
        )
