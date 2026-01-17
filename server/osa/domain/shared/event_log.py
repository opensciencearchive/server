"""EventLog - service for querying the event store (changefeed)."""

from osa.domain.shared.event import Event, EventId
from osa.domain.shared.port.event_repository import EventRepository
from osa.domain.shared.service import Service


class EventLog(Service):
    """Service for querying the event store.

    Provides a changefeed of domain events for federation, replication,
    and viewing event history.
    """

    _repo: EventRepository

    async def list_events(
        self,
        limit: int = 50,
        after: EventId | None = None,
        event_types: list[str] | None = None,
        newest_first: bool = False,
    ) -> list[Event]:
        """List events with cursor-based pagination.

        Args:
            limit: Maximum number of events to return.
            after: Cursor - return events after this event ID.
            event_types: Filter by event type names (e.g., ["RecordPublished"]).
            newest_first: If True, return newest events first.

        Returns:
            List of Events.
        """
        return await self._repo.list_events(
            limit=limit, after=after, event_types=event_types, newest_first=newest_first
        )

    async def count(self, event_types: list[str] | None = None) -> int:
        """Count total events, optionally filtered by types."""
        return await self._repo.count(event_types=event_types)

    async def get(self, event_id: EventId) -> Event | None:
        """Get a single event by ID."""
        return await self._repo.get(event_id)
