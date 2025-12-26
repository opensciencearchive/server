"""Outbox - domain service for reliable event delivery."""

from datetime import UTC, datetime
from typing import TypeVar

from osa.domain.shared.event import Event, EventId
from osa.domain.shared.port.event_repository import EventRepository
from osa.domain.shared.service import Service

E = TypeVar("E", bound=Event)


class Outbox(Service):
    """Domain service for reliable event delivery via the transactional outbox pattern.

    Wraps EventRepository with delivery semantics. Business code uses this
    to append events and query event history. The BackgroundWorker uses this
    to fetch pending events and mark them as delivered/failed.
    """

    _repo: EventRepository

    async def append(self, event: Event) -> None:
        """Add an event to the outbox for delivery."""
        await self._repo.save(event, status="pending")

    async def fetch_pending(self, limit: int = 100) -> list[Event]:
        """Fetch events awaiting delivery."""
        return await self._repo.find_pending(limit)

    async def mark_delivered(self, event_id: EventId) -> None:
        """Mark an event as successfully delivered."""
        await self._repo.update_status(event_id, status="delivered")

    async def mark_failed(self, event_id: EventId, error: str) -> None:
        """Mark an event as failed with an error message."""
        await self._repo.update_status(event_id, status="failed", error=error)

    async def find_latest(self, event_type: type[E]) -> E | None:
        """Find the most recent event of a given type."""
        return await self._repo.find_latest_by_type(event_type)
