"""Outbox - domain service for reliable event delivery."""

from typing import TypeVar

from osa.domain.shared.event import ClaimResult, Event
from osa.domain.shared.model.subscription_registry import SubscriptionRegistry
from osa.domain.shared.port.event_repository import EventRepository
from osa.domain.shared.service import Service

E = TypeVar("E", bound=Event)


class Outbox(Service):
    """Domain service for reliable event delivery via the transactional outbox pattern.

    On append(), queries the SubscriptionRegistry for consumer groups subscribed
    to the event type and creates one delivery row per group. Audit-only events
    (no subscribers) are saved to the log without delivery rows.

    Workers claim deliveries scoped to their consumer group, enabling independent
    processing and failure tracking per handler.
    """

    _repo: EventRepository
    _registry: SubscriptionRegistry

    async def append(self, event: Event, routing_key: str | None = None) -> None:
        """Add an event to the outbox for delivery.

        Creates one delivery row per consumer group subscribed to this event type.
        If no groups are subscribed, the event is saved as audit-only (no deliveries).

        Args:
            event: The event to append.
            routing_key: Optional routing key for worker filtering.
        """
        event_type_name = type(event).__name__
        consumer_groups = self._registry.get(event_type_name, set())
        await self._repo.save_with_deliveries(
            event, consumer_groups=consumer_groups, routing_key=routing_key
        )

    async def claim(
        self,
        event_types: list[type[Event]],
        limit: int,
        consumer_group: str,
    ) -> ClaimResult:
        """Claim pending deliveries for a specific consumer group.

        Uses FOR UPDATE SKIP LOCKED to ensure concurrent workers claim
        different deliveries without blocking.

        Args:
            event_types: Event classes to claim.
            limit: Maximum number of deliveries to claim.
            consumer_group: The handler class name claiming deliveries.

        Returns:
            ClaimResult containing claimed events and timestamp.
        """
        event_type_names = [et.__name__ for et in event_types]
        return await self._repo.claim_delivery(
            consumer_group=consumer_group,
            event_types=event_type_names,
            limit=limit,
        )

    async def mark_delivered(self, delivery_id: str) -> None:
        """Mark a delivery as successfully delivered."""
        await self._repo.mark_delivery_status(delivery_id, status="delivered")

    async def mark_failed(self, delivery_id: str, error: str) -> None:
        """Mark a delivery as failed with an error message."""
        await self._repo.mark_delivery_status(delivery_id, status="failed", error=error)

    async def mark_skipped(self, delivery_id: str, reason: str) -> None:
        """Mark a delivery as skipped (e.g., backend removed)."""
        await self._repo.mark_delivery_status(delivery_id, status="skipped", error=reason)

    async def mark_failed_with_retry(
        self,
        delivery_id: str,
        error: str,
        max_retries: int,
    ) -> None:
        """Mark a delivery as failed, with retry logic.

        If retry_count < max_retries, resets status to pending for retry.
        If retry_count >= max_retries, sets status to failed permanently.

        Args:
            delivery_id: The delivery row ID.
            error: Error message.
            max_retries: Maximum retry attempts before marking as failed.
        """
        await self._repo.mark_failed_with_retry(delivery_id, error=error, max_retries=max_retries)

    async def reset_stale_claims(self, timeout_seconds: float) -> int:
        """Reset deliveries that have been claimed for too long.

        Called periodically to recover from crashed workers.

        Args:
            timeout_seconds: Consider claims older than this as stale.

        Returns:
            Number of deliveries reset.
        """
        return await self._repo.reset_stale_deliveries(timeout_seconds)

    async def find_latest(self, event_type: type[E]) -> E | None:
        """Find the most recent event of a given type."""
        return await self._repo.find_latest_by_type(event_type)

    async def find_latest_where(self, event_type: type[E], **payload_filters: str) -> E | None:
        """Find the most recent event of a given type matching payload field filters."""
        if len(payload_filters) != 1:
            raise ValueError("Exactly one payload filter required")
        field, value = next(iter(payload_filters.items()))
        return await self._repo.find_latest_by_type_and_field(event_type, field, value)
