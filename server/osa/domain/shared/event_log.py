"""EventLog — the event-store changefeed: service + its query handler."""

from datetime import datetime
from typing import Any, Literal

from pydantic import Field

from osa.domain.shared.authorization.gate import public
from osa.domain.shared.event import Event, EventId
from osa.domain.shared.port.event_repository import EventRepository
from osa.domain.shared.query import Query, QueryHandler, Result
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


class ListEvents(Query):
    """Changefeed page request. ``order="asc"`` (oldest first) is the
    federation direction; ``"desc"`` serves recent-events views. Anything else
    is a validation error — never a silent default."""

    limit: int = Field(default=50, ge=1, le=500)
    after: EventId | None = None
    types: list[str] | None = None
    order: Literal["asc", "desc"] = "asc"


class EventEntry(Result):
    """One changefeed event: identity, type discriminator, and the payload
    (the event body minus identity/timestamp, which ride alongside)."""

    id: EventId
    type: str
    created_at: datetime
    data: dict[str, Any]


class EventPage(Result):
    events: list[EventEntry]
    cursor: str | None
    has_more: bool


class ListEventsHandler(QueryHandler[ListEvents, EventPage]):
    """The /events changefeed read (arch-survey 2026-08-16 F1).

    ``public()`` is deliberate, not an omission: the changefeed is the
    federation surface (CLAUDE.md API §9) and mirroring nodes are anonymous.
    Tightening access is a one-line gate change here, boot-validated.
    """

    __auth__ = public()

    event_log: EventLog

    async def run(self, cmd: ListEvents) -> EventPage:
        # limit+1 look-ahead: one surplus row proves a further page exists.
        events = await self.event_log.list_events(
            limit=cmd.limit + 1,
            after=cmd.after,
            event_types=cmd.types,
            newest_first=cmd.order == "desc",
        )
        has_more = len(events) > cmd.limit
        if has_more:
            events = events[: cmd.limit]
        return EventPage(
            events=[
                EventEntry(
                    id=e.id,
                    type=type(e).__name__,
                    created_at=e.created_at,
                    data=e.model_dump(mode="json", exclude={"id", "created_at"}),
                )
                for e in events
            ],
            cursor=str(events[-1].id) if events else None,
            has_more=has_more,
        )
