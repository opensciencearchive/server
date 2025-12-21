"""SQLAlchemy adapter implementing EventRepository."""

from datetime import UTC, datetime
from typing import TypeVar

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.shared.event import Event, EventId
from osa.domain.shared.port.event_repository import EventRepository
from osa.infrastructure.persistence.tables import events_table

E = TypeVar("E", bound=Event)


class SQLAlchemyEventRepository(EventRepository):
    """SQLAlchemy-backed event repository.

    Pure CRUD operations. Delivery semantics are handled by Outbox.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(self, event: Event, status: str = "pending") -> None:
        """Persist an event."""
        stmt = insert(events_table).values(
            id=str(event.id),
            event_type=type(event).__name__,
            payload=event.model_dump(mode="json"),
            created_at=datetime.now(UTC),
            delivery_status=status,
        )
        await self._session.execute(stmt)

    async def get(self, event_id: EventId) -> Event | None:
        """Get an event by ID."""
        stmt = select(
            events_table.c.event_type,
            events_table.c.payload,
        ).where(events_table.c.id == str(event_id))

        result = await self._session.execute(stmt)
        row = result.first()

        if row is None:
            return None

        event_type, payload = row
        return self._deserialize(event_type, payload)

    async def update_status(
        self,
        event_id: EventId,
        status: str,
        error: str | None = None,
    ) -> None:
        """Update an event's delivery status."""
        values: dict = {
            "delivery_status": status,
            "delivered_at": datetime.now(UTC),
        }
        if error is not None:
            values["delivery_error"] = error

        stmt = (
            update(events_table)
            .where(events_table.c.id == str(event_id))
            .values(**values)
        )
        await self._session.execute(stmt)

    async def find_pending(self, limit: int = 100) -> list[Event]:
        """Find events with pending status."""
        stmt = (
            select(events_table.c.event_type, events_table.c.payload)
            .where(events_table.c.delivery_status == "pending")
            .order_by(events_table.c.created_at.asc())
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        rows = result.fetchall()

        events: list[Event] = []
        for event_type, payload in rows:
            event = self._deserialize(event_type, payload)
            if event is not None:
                events.append(event)
        return events

    async def find_latest_by_type(self, event_type: type[E]) -> E | None:
        """Find the most recent event of a given type."""
        type_name = event_type.__name__

        stmt = (
            select(events_table.c.payload)
            .where(events_table.c.event_type == type_name)
            .order_by(events_table.c.created_at.desc())
            .limit(1)
        )

        result = await self._session.execute(stmt)
        row = result.first()

        if row is None:
            return None

        payload = row[0]
        return self._deserialize(type_name, payload)  # type: ignore[return-value]

    def _deserialize(self, event_type: str, payload: dict | str) -> Event | None:
        """Deserialize an event from stored data."""
        event_cls = Event._registry.get(event_type)
        if event_cls is None:
            return None

        # Handle both dict and string payloads (SQLite JSON vs text)
        if isinstance(payload, str):
            return event_cls.model_validate_json(payload)
        return event_cls.model_validate(payload)
