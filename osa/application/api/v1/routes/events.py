"""Events API routes - changefeed for federation."""

from datetime import datetime
from uuid import UUID

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter, Query
from pydantic import BaseModel

from osa.domain.shared.event import EventId
from osa.domain.shared.event_log import EventLog

router = APIRouter(
    prefix="/events",
    tags=["events"],
    route_class=DishkaRoute,
)


class EventResponse(BaseModel):
    """Single event in the response."""

    id: UUID
    type: str
    created_at: datetime
    data: dict


class EventListResponse(BaseModel):
    """Response for listing events."""

    events: list[EventResponse]
    cursor: str | None
    has_more: bool


@router.get("")
async def list_events(
    event_log: FromDishka[EventLog],
    limit: int = Query(50, ge=1, le=500, description="Maximum number of events"),
    after: UUID | None = Query(None, description="Cursor: return events after this ID"),
    types: list[str] | None = Query(None, description="Filter by event types"),
    order: str = Query("asc", description="Order: 'asc' (oldest first) or 'desc' (newest first)"),
) -> EventListResponse:
    """List events from the event log (changefeed).

    Use order=asc (default) for federation, order=desc for viewing recent events.
    Use the cursor to paginate through results.
    """
    newest_first = order == "desc"
    after_id = EventId(after) if after else None
    events = await event_log.list_events(
        limit=limit + 1, after=after_id, event_types=types, newest_first=newest_first
    )

    # Check if there are more results
    has_more = len(events) > limit
    if has_more:
        events = events[:limit]

    # Cursor is the ID of the last event
    cursor = str(events[-1].id) if events else None

    return EventListResponse(
        events=[
            EventResponse(
                id=e.id,
                type=type(e).__name__,
                created_at=e.created_at,
                data=e.model_dump(mode="json", exclude={"id", "created_at"}),
            )
            for e in events
        ],
        cursor=cursor,
        has_more=has_more,
    )
