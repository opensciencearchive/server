"""SQLAlchemy adapter implementing EventRepository."""

import logging
from datetime import UTC, datetime, timedelta
from typing import TypeVar

from sqlalchemy import func, insert, or_, select, update
from sqlalchemy.dialects.postgresql import INTERVAL
from sqlalchemy.sql import literal
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.shared.event import ClaimResult, Event, EventId
from osa.domain.shared.port.event_repository import EventRepository
from osa.infrastructure.persistence.tables import events_table

logger = logging.getLogger(__name__)

E = TypeVar("E", bound=Event)


class SQLAlchemyEventRepository(EventRepository):
    """SQLAlchemy-backed event repository.

    Pure CRUD operations. Delivery semantics are handled by Outbox.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(
        self, event: Event, status: str = "pending", routing_key: str | None = None
    ) -> None:
        """Persist an event."""
        now = datetime.now(UTC)
        stmt = insert(events_table).values(
            id=str(event.id),
            event_type=type(event).__name__,
            payload=event.model_dump(mode="json"),
            created_at=now,
            delivery_status=status,
            routing_key=routing_key,
            retry_count=0,
            updated_at=now,
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
        now = datetime.now(UTC)
        values: dict = {
            "delivery_status": status,
            "delivered_at": now,
            "updated_at": now,
        }
        if error is not None:
            values["delivery_error"] = error

        stmt = update(events_table).where(events_table.c.id == str(event_id)).values(**values)
        await self._session.execute(stmt)

    async def find_pending(self, limit: int = 100, fair: bool = True) -> list[Event]:
        """Find events with pending status.

        Args:
            limit: Maximum number of events to return.
            fair: If True, fetch equally from each event type (round-robin).
                  If False, use strict FIFO ordering (oldest first).
        """
        if not fair:
            # Strict FIFO: oldest events first regardless of type
            stmt = (
                select(events_table.c.event_type, events_table.c.payload)
                .where(events_table.c.delivery_status == "pending")
                .order_by(events_table.c.created_at.asc())
                .limit(limit)
            )
        else:
            # Round-robin: fetch equally from each event type
            # Uses window function to rank events within each type by created_at
            row_num = (
                func.row_number()
                .over(
                    partition_by=events_table.c.event_type,
                    order_by=events_table.c.created_at.asc(),
                )
                .label("rn")
            )

            subq = (
                select(
                    events_table.c.event_type,
                    events_table.c.payload,
                    events_table.c.created_at,
                    row_num,
                ).where(events_table.c.delivery_status == "pending")
            ).subquery()

            # Order by rank first (ensures round-robin), then by created_at
            # This interleaves: rank 1 of each type, then rank 2 of each type, etc.
            stmt = (
                select(subq.c.event_type, subq.c.payload)
                .order_by(subq.c.rn, subq.c.created_at)
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

        # Order by created_at
        if newest_first:
            stmt = stmt.order_by(events_table.c.created_at.desc())
        else:
            stmt = stmt.order_by(events_table.c.created_at.asc())

        # Cursor: get events after the given ID
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

    async def claim(
        self,
        event_types: list[str],
        limit: int,
        routing_key: str | None = None,
    ) -> ClaimResult:
        """Claim pending events using FOR UPDATE SKIP LOCKED.

        This atomically selects and locks events for processing. Concurrent
        workers will skip already-locked events.

        Args:
            event_types: Event type names to claim.
            limit: Maximum number of events to claim.
            routing_key: Optional routing key filter.

        Returns:
            ClaimResult containing claimed events and timestamp.
        """
        now = datetime.now(UTC)

        # Build WHERE clause for pending events
        # Include events that are pending and eligible for retry (based on backoff)
        where_clauses = [
            events_table.c.delivery_status == "pending",
            events_table.c.event_type.in_(event_types),
        ]

        # Routing key filter
        if routing_key is not None:
            where_clauses.append(events_table.c.routing_key == routing_key)
        else:
            # When routing_key is None, only claim unrouted events
            where_clauses.append(events_table.c.routing_key.is_(None))

        # Backoff eligibility: either first attempt (retry_count=0) or enough time passed
        # Backoff formula: min(30, 5^retry_count) seconds
        # Events must have updated_at <= now - backoff_seconds to be eligible
        backoff_seconds = func.least(
            literal(30),
            func.power(literal(5), events_table.c.retry_count),
        )
        backoff_interval = func.cast(func.concat(backoff_seconds, literal(" seconds")), INTERVAL)
        backoff_eligible = or_(
            events_table.c.retry_count == 0,
            events_table.c.updated_at <= func.now() - backoff_interval,
        )
        where_clauses.append(backoff_eligible)

        # Select with FOR UPDATE SKIP LOCKED
        stmt = (
            select(events_table.c.id, events_table.c.event_type, events_table.c.payload)
            .where(*where_clauses)
            .order_by(events_table.c.created_at.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )

        result = await self._session.execute(stmt)
        rows = result.fetchall()

        if not rows:
            return ClaimResult(events=[], claimed_at=now)

        # Update status to 'claimed' and set claimed_at
        event_ids = [row[0] for row in rows]
        update_stmt = (
            update(events_table)
            .where(events_table.c.id.in_(event_ids))
            .values(delivery_status="claimed", claimed_at=now, updated_at=now)
        )
        await self._session.execute(update_stmt)

        # Deserialize events
        events: list[Event] = []
        for row in rows:
            _, event_type, payload = row
            event = self._deserialize(event_type, payload)
            if event is not None:
                events.append(event)

        return ClaimResult(events=events, claimed_at=now)

    async def reset_stale_claims(self, timeout_seconds: float) -> int:
        """Reset events that have been claimed for too long.

        Args:
            timeout_seconds: Consider claims older than this as stale.

        Returns:
            Number of events reset.
        """
        cutoff = datetime.now(UTC) - timedelta(seconds=timeout_seconds)

        stmt = (
            update(events_table)
            .where(
                events_table.c.delivery_status == "claimed",
                events_table.c.claimed_at < cutoff,
            )
            .values(
                delivery_status="pending",
                claimed_at=None,
                updated_at=datetime.now(UTC),
            )
        )

        result = await self._session.execute(stmt)
        count = result.rowcount
        if count > 0:
            logger.info(f"Reset {count} stale claims (older than {timeout_seconds}s)")
        return count

    async def mark_failed_with_retry(
        self,
        event_id: EventId,
        error: str,
        max_retries: int,
    ) -> None:
        """Mark an event as failed with retry logic.

        If retry_count < max_retries, increments retry_count and resets
        status to 'pending' for retry.
        If retry_count >= max_retries, sets status to 'failed' permanently.
        """
        now = datetime.now(UTC)

        # First, get the current retry_count
        select_stmt = select(events_table.c.retry_count).where(events_table.c.id == str(event_id))
        result = await self._session.execute(select_stmt)
        row = result.first()

        if row is None:
            logger.warning(f"Event {event_id} not found for mark_failed_with_retry")
            return

        current_retry_count = row[0] or 0
        new_retry_count = current_retry_count + 1

        if new_retry_count >= max_retries:
            # Exceeded max retries - mark as permanently failed
            update_stmt = (
                update(events_table)
                .where(events_table.c.id == str(event_id))
                .values(
                    delivery_status="failed",
                    delivery_error=error,
                    retry_count=new_retry_count,
                    updated_at=now,
                    delivered_at=now,
                )
            )
        else:
            # Reset to pending for retry
            update_stmt = (
                update(events_table)
                .where(events_table.c.id == str(event_id))
                .values(
                    delivery_status="pending",
                    delivery_error=error,
                    retry_count=new_retry_count,
                    claimed_at=None,
                    updated_at=now,
                )
            )

        await self._session.execute(update_stmt)
