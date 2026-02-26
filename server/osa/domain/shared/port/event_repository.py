"""EventRepository port - pure CRUD for event persistence."""

from typing import Protocol, TypeVar

from osa.domain.shared.event import ClaimResult, Event, EventId

E = TypeVar("E", bound=Event)


class EventRepository(Protocol):
    """Repository for domain events - pure data access.

    Events are stored in an append-only log. Delivery tracking is handled
    via a separate deliveries table, one row per (event, consumer_group) pair.
    """

    async def save_with_deliveries(
        self,
        event: Event,
        consumer_groups: set[str],
        routing_key: str | None = None,
    ) -> None:
        """Save event to the append-only log and create delivery rows.

        Args:
            event: The event to persist.
            consumer_groups: Set of consumer group names to create deliveries for.
                If empty, the event is saved without any delivery rows (audit-only).
            routing_key: Optional routing key stored on delivery rows for filtering.
        """
        ...

    async def get(self, event_id: EventId) -> Event | None:
        """Get an event by ID."""
        ...

    async def find_latest_by_type(self, event_type: type[E]) -> E | None:
        """Find the most recent event of a given type."""
        ...

    async def find_latest_by_type_and_field(
        self, event_type: type[E], field: str, value: str
    ) -> E | None:
        """Find the most recent event of a given type where payload->>field = value."""
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

    async def claim_delivery(
        self,
        consumer_group: str,
        event_types: list[str],
        limit: int = 1,
    ) -> ClaimResult:
        """Claim pending deliveries for a specific consumer group.

        Atomically selects and locks delivery rows using FOR UPDATE SKIP LOCKED.
        Joins to the events table to return the full event payload.

        Args:
            consumer_group: The handler class name claiming deliveries.
            event_types: Event type names to claim.
            limit: Maximum deliveries to claim.

        Returns:
            ClaimResult containing claimed events and timestamp.
        """
        ...

    async def mark_delivery_status(
        self,
        delivery_id: str,
        status: str,
        error: str | None = None,
    ) -> None:
        """Update a delivery's status.

        Args:
            delivery_id: The delivery row ID.
            status: New status (delivered, failed, skipped).
            error: Optional error message for failed/skipped.
        """
        ...

    async def reset_stale_deliveries(self, timeout_seconds: float) -> int:
        """Reset deliveries that have been claimed for too long.

        Sets status back to 'pending' for deliveries where:
        - status = 'claimed'
        - claimed_at < now() - timeout_seconds

        Args:
            timeout_seconds: Consider claims older than this as stale.

        Returns:
            Number of deliveries reset.
        """
        ...

    async def mark_failed_with_retry(
        self,
        delivery_id: str,
        error: str,
        max_retries: int,
    ) -> None:
        """Mark a delivery as failed with retry logic.

        If retry_count < max_retries, increments retry_count and resets
        status to 'pending' for retry.
        If retry_count >= max_retries, sets status to 'failed' permanently.

        Args:
            delivery_id: The delivery row ID.
            error: Error message to record.
            max_retries: Maximum retry attempts before marking as failed.
        """
        ...
