"""SQLAlchemy adapter implementing EventRepository."""

import logging
from datetime import UTC, datetime, timedelta
from typing import TypeVar
from uuid import uuid4

from sqlalchemy import func, insert, or_, select, update
from sqlalchemy.dialects.postgresql import INTERVAL
from sqlalchemy.sql import literal
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.shared.event import ClaimResult, Event, EventId
from osa.domain.shared.port.event_repository import EventRepository
from osa.infrastructure.persistence.tables import deliveries_table, events_table

logger = logging.getLogger(__name__)

E = TypeVar("E", bound=Event)


class SQLAlchemyEventRepository(EventRepository):
    """SQLAlchemy-backed event repository.

    Events are stored in an append-only log. Delivery tracking uses a
    separate deliveries table with one row per (event, consumer_group) pair.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save_with_deliveries(
        self,
        event: Event,
        consumer_groups: set[str],
        routing_key: str | None = None,
    ) -> None:
        """Save event to append-only log and create delivery rows."""
        now = datetime.now(UTC)

        # Insert event into append-only events table
        event_stmt = insert(events_table).values(
            id=str(event.id),
            event_type=type(event).__name__,
            payload=event.model_dump(mode="json"),
            created_at=now,
        )
        await self._session.execute(event_stmt)

        # Create one delivery row per consumer group
        for group in consumer_groups:
            delivery_stmt = insert(deliveries_table).values(
                id=str(uuid4()),
                event_id=str(event.id),
                consumer_group=group,
                status="pending",
                retry_count=0,
                updated_at=now,
            )
            await self._session.execute(delivery_stmt)

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

        (payload,) = row
        return self._deserialize(type_name, payload)  # type: ignore[return-value]

    async def find_latest_by_type_and_field(
        self, event_type: type[E], field: str, value: str
    ) -> E | None:
        """Find the most recent event of a given type where payload->>field = value."""
        type_name = event_type.__name__

        stmt = (
            select(events_table.c.payload)
            .where(
                events_table.c.event_type == type_name,
                events_table.c.payload[field].as_string() == value,
            )
            .order_by(events_table.c.created_at.desc())
            .limit(1)
        )

        result = await self._session.execute(stmt)
        row = result.first()

        if row is None:
            return None

        (payload,) = row
        return self._deserialize(type_name, payload)  # type: ignore[return-value]

    async def list_events(
        self,
        limit: int = 50,
        after: EventId | None = None,
        event_types: list[str] | None = None,
        newest_first: bool = False,
    ) -> list[Event]:
        """List events with cursor-based pagination."""
        stmt = select(
            events_table.c.event_type,
            events_table.c.payload,
        )

        if newest_first:
            stmt = stmt.order_by(events_table.c.created_at.desc())
        else:
            stmt = stmt.order_by(events_table.c.created_at.asc())

        if after is not None:
            cursor_stmt = select(events_table.c.created_at).where(events_table.c.id == str(after))
            cursor_result = await self._session.execute(cursor_stmt)
            cursor_row = cursor_result.first()
            if cursor_row:
                if newest_first:
                    stmt = stmt.where(events_table.c.created_at < cursor_row[0])
                else:
                    stmt = stmt.where(events_table.c.created_at > cursor_row[0])

        if event_types:
            stmt = stmt.where(events_table.c.event_type.in_(event_types))

        stmt = stmt.limit(limit)

        result = await self._session.execute(stmt)
        rows = result.fetchall()

        events: list[Event] = []
        for event_type, payload in rows:
            event = self._deserialize(event_type, payload)
            if event is not None:
                events.append(event)
        return events

    async def count(self, event_types: list[str] | None = None) -> int:
        """Count events, optionally filtered by types."""
        stmt = select(func.count()).select_from(events_table)

        if event_types:
            stmt = stmt.where(events_table.c.event_type.in_(event_types))

        result = await self._session.execute(stmt)
        return result.scalar() or 0

    async def claim_delivery(
        self,
        consumer_group: str,
        event_types: list[str],
        limit: int = 1,
    ) -> ClaimResult:
        """Claim pending deliveries for a specific consumer group.

        Uses FOR UPDATE SKIP LOCKED on the deliveries table, joining to
        events to filter by event_type and return the full event payload.
        """
        now = datetime.now(UTC)

        # Backoff formula: min(30, 5^retry_count) seconds
        backoff_seconds = func.least(
            literal(30),
            func.power(literal(5), deliveries_table.c.retry_count),
        )
        backoff_interval = func.cast(func.concat(backoff_seconds, literal(" seconds")), INTERVAL)
        backoff_eligible = or_(
            deliveries_table.c.retry_count == 0,
            deliveries_table.c.updated_at <= func.now() - backoff_interval,
        )

        # Select deliveries joined with events
        stmt = (
            select(
                deliveries_table.c.id,
                events_table.c.event_type,
                events_table.c.payload,
            )
            .join(events_table, deliveries_table.c.event_id == events_table.c.id)
            .where(
                deliveries_table.c.consumer_group == consumer_group,
                deliveries_table.c.status == "pending",
                events_table.c.event_type.in_(event_types),
                backoff_eligible,
            )
            .order_by(events_table.c.created_at.asc())
            .limit(limit)
            .with_for_update(skip_locked=True, of=deliveries_table)
        )

        result = await self._session.execute(stmt)
        rows = result.fetchall()

        if not rows:
            return ClaimResult(events=[], claimed_at=now)

        # Update delivery status to 'claimed'
        delivery_ids = [row[0] for row in rows]
        update_stmt = (
            update(deliveries_table)
            .where(deliveries_table.c.id.in_(delivery_ids))
            .values(status="claimed", claimed_at=now, updated_at=now)
        )
        await self._session.execute(update_stmt)

        # Deserialize events, attaching delivery_id for later mark operations
        events: list[Event] = []
        for row in rows:
            delivery_id, event_type, payload = row
            event = self._deserialize(event_type, payload)
            if event is not None:
                # Store delivery_id on the event for the Worker to use
                event._delivery_id = delivery_id  # type: ignore[attr-defined]
                events.append(event)

        return ClaimResult(events=events, claimed_at=now)

    async def mark_delivery_status(
        self,
        delivery_id: str,
        status: str,
        error: str | None = None,
    ) -> None:
        """Update a delivery's status."""
        now = datetime.now(UTC)
        values: dict = {
            "status": status,
            "updated_at": now,
        }
        if status == "delivered":
            values["delivered_at"] = now
        if error is not None:
            values["delivery_error"] = error

        stmt = update(deliveries_table).where(deliveries_table.c.id == delivery_id).values(**values)
        await self._session.execute(stmt)

    async def reset_stale_deliveries(self, timeout_seconds: float) -> int:
        """Reset deliveries that have been claimed for too long."""
        cutoff = datetime.now(UTC) - timedelta(seconds=timeout_seconds)

        stmt = (
            update(deliveries_table)
            .where(
                deliveries_table.c.status == "claimed",
                deliveries_table.c.claimed_at < cutoff,
            )
            .values(
                status="pending",
                claimed_at=None,
                updated_at=datetime.now(UTC),
            )
        )

        result = await self._session.execute(stmt)
        count = result.rowcount
        if count > 0:
            logger.info(f"Reset {count} stale deliveries (older than {timeout_seconds}s)")
        return count

    async def mark_failed_with_retry(
        self,
        delivery_id: str,
        error: str,
        max_retries: int,
    ) -> None:
        """Mark a delivery as failed with retry logic."""
        now = datetime.now(UTC)

        # Get current retry_count
        select_stmt = select(deliveries_table.c.retry_count).where(
            deliveries_table.c.id == delivery_id
        )
        result = await self._session.execute(select_stmt)
        row = result.first()

        if row is None:
            logger.warning(f"Delivery {delivery_id} not found for mark_failed_with_retry")
            return

        current_retry_count = row[0] or 0
        new_retry_count = current_retry_count + 1

        if new_retry_count >= max_retries:
            # Exceeded max retries - mark as permanently failed
            update_stmt = (
                update(deliveries_table)
                .where(deliveries_table.c.id == delivery_id)
                .values(
                    status="failed",
                    delivery_error=error,
                    retry_count=new_retry_count,
                    updated_at=now,
                    delivered_at=now,
                )
            )
        else:
            # Reset to pending for retry
            update_stmt = (
                update(deliveries_table)
                .where(deliveries_table.c.id == delivery_id)
                .values(
                    status="pending",
                    delivery_error=error,
                    retry_count=new_retry_count,
                    claimed_at=None,
                    updated_at=now,
                )
            )

        await self._session.execute(update_stmt)

    def _deserialize(self, event_type: str, payload: dict | str) -> Event | None:
        """Deserialize an event from stored data."""
        event_cls = Event._registry.get(event_type)
        if event_cls is None:
            logger.warning(f"Unknown event type '{event_type}' - skipping")
            return None

        try:
            if isinstance(payload, str):
                return event_cls.model_validate_json(payload)
            return event_cls.model_validate(payload)
        except Exception as e:
            logger.error(f"Failed to deserialize event type '{event_type}': {e}")
            return None
