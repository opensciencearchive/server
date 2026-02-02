"""EventRepository port - pure CRUD for event persistence."""

from typing import Protocol, TypeVar

from osa.domain.shared.event import ClaimResult, Event, EventId

E = TypeVar("E", bound=Event)


class EventRepository(Protocol):
    """Repository for domain events - pure data access.

    Delivery semantics (pending/delivered/failed) are handled by the Outbox service.
    """

    async def save(
        self, event: Event, status: str = "pending", routing_key: str | None = None
    ) -> None:
        """Persist an event with initial status and optional routing key."""
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

    async def find_pending(self, limit: int = 100, fair: bool = True) -> list[Event]:
        """Find events with pending status.

        Args:
            limit: Maximum number of events to return.
            fair: If True, fetch equally from each event type (round-robin).
                  If False, use strict FIFO ordering (oldest first).
        """
        ...

    async def find_latest_by_type(self, event_type: type[E]) -> E | None:
        """Find the most recent event of a given type."""
        ...

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
            newest_first: If True, return newest events first (for CLI).
                         If False, return oldest first (for federation).

        Returns:
            List of Events.
        """
        ...

    async def count(self, event_types: list[str] | None = None) -> int:
        """Count events, optionally filtered by types."""
        ...

    async def claim(
        self,
        event_types: list[str],
        limit: int,
        routing_key: str | None = None,
    ) -> ClaimResult:
        """Claim pending events for processing using FOR UPDATE SKIP LOCKED.

        This atomically:
        1. Selects pending events matching event_types and routing_key
        2. Locks them with FOR UPDATE SKIP LOCKED (concurrent workers skip)
        3. Sets status to 'claimed' and claimed_at to current timestamp

        Args:
            event_types: Event type names to claim (class names).
            limit: Maximum number of events to claim.
            routing_key: Optional routing key filter. If None, claims unrouted events only.

        Returns:
            ClaimResult containing claimed events and timestamp.
        """
        ...

    async def reset_stale_claims(self, timeout_seconds: float) -> int:
        """Reset events that have been claimed for longer than timeout.

        Sets status back to 'pending' for events where:
        - status = 'claimed'
        - claimed_at < now() - timeout_seconds

        Args:
            timeout_seconds: Consider claims older than this as stale.

        Returns:
            Number of events reset.
        """
        ...

    async def mark_failed_with_retry(
        self,
        event_id: "EventId",
        error: str,
        max_retries: int,
    ) -> None:
        """Mark an event as failed with retry logic.

        If retry_count < max_retries, increments retry_count and resets
        status to 'pending' for retry.
        If retry_count >= max_retries, sets status to 'failed' permanently.

        Args:
            event_id: The event ID.
            error: Error message to record.
            max_retries: Maximum retry attempts before marking as failed.
        """
        ...
