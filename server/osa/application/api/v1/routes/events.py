"""Events API routes - changefeed for federation.

Thin HTTP ↔ DTO coercion only: the changefeed read (look-ahead pagination,
payload shaping) and its explicit ``public()`` gate live in
``ListEventsHandler`` (arch-survey 2026-08-16 F1 — this route previously
injected the EventLog service directly, bypassing the gate machinery).
"""

from datetime import datetime
from typing import Literal
from uuid import UUID

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter, Query
from pydantic import BaseModel

from osa.domain.shared.event import EventId
from osa.domain.shared.event_log import ListEvents, ListEventsHandler

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
    handler: FromDishka[ListEventsHandler],
    limit: int = Query(50, ge=1, le=500, description="Maximum number of events"),
    after: UUID | None = Query(None, description="Cursor: return events after this ID"),
    types: list[str] | None = Query(None, description="Filter by event types"),
    order: Literal["asc", "desc"] = Query(
        "asc", description="'asc' (oldest first, federation) or 'desc' (newest first)"
    ),
) -> EventListResponse:
    """List events from the event log (changefeed).

    Use order=asc (default) for federation, order=desc for viewing recent events.
    Use the cursor to paginate through results.
    """
    page = await handler.run(
        ListEvents(
            limit=limit,
            after=EventId(after) if after else None,
            types=types,
            order=order,
        )
    )
    return EventListResponse(
        events=[
            EventResponse(id=e.id, type=e.type, created_at=e.created_at, data=e.data)
            for e in page.events
        ],
        cursor=page.cursor,
        has_more=page.has_more,
    )
