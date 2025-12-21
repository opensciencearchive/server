"""EventRepository port - pure CRUD for event persistence."""

from typing import Protocol, TypeVar

from osa.domain.shared.event import Event, EventId

E = TypeVar("E", bound=Event)


class EventRepository(Protocol):
    """Repository for domain events - pure data access.

    Delivery semantics (pending/delivered/failed) are handled by the Outbox service.
    """

    async def save(self, event: Event, status: str = "pending") -> None:
        """Persist an event with initial status."""
        ...

    async def get(self, event_id: EventId) -> Event | None:
        """Get an event by ID."""
        ...

    async def update_status(
        self,
        event_id: EventId,
        status: str,
        error: str | None = None,
    ) -> None:
        """Update an event's delivery status."""
        ...

    async def find_pending(self, limit: int = 100) -> list[Event]:
        """Find events with pending status."""
        ...

    async def find_latest_by_type(self, event_type: type[E]) -> E | None:
        """Find the most recent event of a given type."""
        ...
