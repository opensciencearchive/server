"""Outbox - domain service for reliable event delivery."""

from typing import TypeVar

from osa.domain.shared.event import ClaimResult, Event, EventId
from osa.domain.shared.port.event_repository import EventRepository
from osa.domain.shared.service import Service

E = TypeVar("E", bound=Event)


class Outbox(Service):
    """Domain service for reliable event delivery via the transactional outbox pattern.

    Wraps EventRepository with delivery semantics. Business code uses this
    to append events and query event history. Workers use this to claim
    events for processing and mark them as delivered/failed.
    """

    _repo: EventRepository

    async def append(self, event: Event, routing_key: str | None = None) -> None:
        """Add an event to the outbox for delivery.

        Args:
            event: The event to append.
            routing_key: Optional routing key for worker filtering.
        """
        await self._repo.save(event, status="pending", routing_key=routing_key)

    async def fetch_pending(self, limit: int = 100, fair: bool = True) -> list[Event]:
        """Fetch events awaiting delivery.

        Args:
            limit: Maximum events to return.
            fair: If True, round-robin across event types. If False, strict FIFO.
        """
        return await self._repo.find_pending(limit, fair=fair)

    async def mark_delivered(self, event_id: EventId) -> None:
        """Mark an event as successfully delivered."""
        await self._repo.update_status(event_id, status="delivered")

    async def mark_failed(self, event_id: EventId, error: str) -> None:
        """Mark an event as failed with an error message."""
        await self._repo.update_status(event_id, status="failed", error=error)

    async def mark_skipped(self, event_id: EventId, reason: str) -> None:
        """Mark an event as skipped (e.g., backend removed)."""
        await self._repo.update_status(event_id, status="skipped", error=reason)

    async def find_latest(self, event_type: type[E]) -> E | None:
        """Find the most recent event of a given type."""
        return await self._repo.find_latest_by_type(event_type)

    async def find_latest_where(self, event_type: type[E], **payload_filters: str) -> E | None:
        """Find the most recent event of a given type matching payload field filters."""
        if len(payload_filters) != 1:
            raise ValueError("Exactly one payload filter required")
        field, value = next(iter(payload_filters.items()))
        return await self._repo.find_latest_by_type_and_field(event_type, field, value)

    async def claim(
        self,
        event_types: list[type[Event]],
        limit: int,
        routing_key: str | None = None,
    ) -> ClaimResult:
        """Claim pending events for processing.

        Uses FOR UPDATE SKIP LOCKED to ensure concurrent workers claim
        different events without blocking.

        Args:
            event_types: Event classes to claim.
            limit: Maximum number of events to claim.
            routing_key: Optional routing key filter.

        Returns:
            ClaimResult containing claimed events and timestamp.
        """
        event_type_names = [et.__name__ for et in event_types]
        return await self._repo.claim(
            event_types=event_type_names,
            limit=limit,
            routing_key=routing_key,
        )

    async def mark_failed_with_retry(
        self,
        event_id: EventId,
        error: str,
        max_retries: int,
    ) -> None:
        """Mark an event as failed, with retry logic.

        If retry_count < max_retries, resets status to pending for retry.
        If retry_count >= max_retries, sets status to failed permanently.

        Args:
            event_id: The event ID.
            error: Error message.
            max_retries: Maximum retry attempts before marking as failed.
        """
        await self._repo.mark_failed_with_retry(event_id, error=error, max_retries=max_retries)

    async def reset_stale_claims(self, timeout_seconds: float) -> int:
        """Reset events that have been claimed for too long.

        Called periodically to recover from crashed workers.

        Args:
            timeout_seconds: Consider claims older than this as stale.

        Returns:
            Number of events reset.
        """
        return await self._repo.reset_stale_claims(timeout_seconds)
