"""Unit tests for consumer-group delivery model.

Tests for User Story 1: Multiple Handlers Reliably Process the Same Event.
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from osa.domain.shared.event import ClaimResult, Event, EventId
from osa.domain.shared.model.subscription_registry import SubscriptionRegistry
from osa.domain.shared.outbox import Outbox


class DummyEvent(Event):
    """Test event for consumer-group tests."""

    id: EventId
    data: str


class AnotherEvent(Event):
    """Second test event for multi-type tests."""

    id: EventId
    value: int


class TestOutboxAppendCreatesDeliveries:
    """T004: Test that outbox.append() creates N delivery rows for N consumer groups."""

    @pytest.fixture
    def mock_repo(self) -> AsyncMock:
        return AsyncMock()

    @pytest.fixture
    def registry(self) -> SubscriptionRegistry:
        return SubscriptionRegistry(
            {"DummyEvent": {"HandlerA", "HandlerB"}, "AnotherEvent": {"HandlerC"}}
        )

    @pytest.fixture
    def outbox(self, mock_repo: AsyncMock, registry: SubscriptionRegistry) -> Outbox:
        outbox = Outbox.__new__(Outbox)
        outbox._repo = mock_repo
        outbox._registry = registry
        return outbox

    async def test_append_creates_deliveries_for_subscribed_groups(
        self, outbox: Outbox, mock_repo: AsyncMock
    ):
        """append() should create one delivery per consumer group subscribed to the event type."""
        event = DummyEvent(id=EventId(uuid4()), data="test")

        await outbox.append(event)

        mock_repo.save_with_deliveries.assert_called_once_with(
            event, consumer_groups={"HandlerA", "HandlerB"}, routing_key=None
        )

    async def test_append_with_routing_key_passes_through(
        self, outbox: Outbox, mock_repo: AsyncMock
    ):
        """append() should pass routing_key through to save_with_deliveries."""
        event = DummyEvent(id=EventId(uuid4()), data="routed")

        await outbox.append(event, routing_key="vector")

        mock_repo.save_with_deliveries.assert_called_once_with(
            event, consumer_groups={"HandlerA", "HandlerB"}, routing_key="vector"
        )

    async def test_append_audit_only_event_creates_zero_deliveries(
        self, outbox: Outbox, mock_repo: AsyncMock
    ):
        """append() for an event with no subscribers creates zero delivery rows."""

        # AuditEvent is not in the registry
        class AuditEvent(Event):
            id: EventId

        event = AuditEvent(id=EventId(uuid4()))

        await outbox.append(event)

        mock_repo.save_with_deliveries.assert_called_once_with(
            event, consumer_groups=set(), routing_key=None
        )


class TestOutboxClaimByConsumerGroup:
    """T005: Test that outbox.claim() returns only deliveries for the calling consumer group."""

    @pytest.fixture
    def mock_repo(self) -> AsyncMock:
        return AsyncMock()

    @pytest.fixture
    def registry(self) -> SubscriptionRegistry:
        return SubscriptionRegistry({"DummyEvent": {"HandlerA", "HandlerB"}})

    @pytest.fixture
    def outbox(self, mock_repo: AsyncMock, registry: SubscriptionRegistry) -> Outbox:
        outbox = Outbox.__new__(Outbox)
        outbox._repo = mock_repo
        outbox._registry = registry
        return outbox

    async def test_claim_passes_consumer_group_to_repo(self, outbox: Outbox, mock_repo: AsyncMock):
        """claim() should pass consumer_group to repository's claim_delivery method."""
        now = datetime.now(UTC)
        mock_repo.claim_delivery.return_value = ClaimResult(events=[], claimed_at=now)

        await outbox.claim(
            event_types=[DummyEvent],
            limit=10,
            consumer_group="HandlerA",
        )

        mock_repo.claim_delivery.assert_called_once_with(
            consumer_group="HandlerA",
            event_types=["DummyEvent"],
            limit=10,
        )

    async def test_claim_returns_events_for_calling_group(
        self, outbox: Outbox, mock_repo: AsyncMock
    ):
        """claim() returns only events assigned to the calling consumer group."""
        event = DummyEvent(id=EventId(uuid4()), data="test")
        now = datetime.now(UTC)
        mock_repo.claim_delivery.return_value = ClaimResult(events=[event], claimed_at=now)

        result = await outbox.claim(
            event_types=[DummyEvent],
            limit=10,
            consumer_group="HandlerA",
        )

        assert len(result) == 1
        assert result.events[0] is event


class TestIndependentFailureTracking:
    """T006: Test that consumer A fails, consumer B succeeds independently."""

    @pytest.fixture
    def mock_repo(self) -> AsyncMock:
        return AsyncMock()

    @pytest.fixture
    def registry(self) -> SubscriptionRegistry:
        return SubscriptionRegistry({"DummyEvent": {"HandlerA", "HandlerB"}})

    @pytest.fixture
    def outbox(self, mock_repo: AsyncMock, registry: SubscriptionRegistry) -> Outbox:
        outbox = Outbox.__new__(Outbox)
        outbox._repo = mock_repo
        outbox._registry = registry
        return outbox

    async def test_mark_delivered_uses_delivery_id(self, outbox: Outbox, mock_repo: AsyncMock):
        """mark_delivered() should operate on delivery_id, not event_id."""
        delivery_id = str(uuid4())

        await outbox.mark_delivered(delivery_id)

        mock_repo.mark_delivery_status.assert_called_once_with(delivery_id, status="delivered")

    async def test_mark_failed_uses_delivery_id(self, outbox: Outbox, mock_repo: AsyncMock):
        """mark_failed() should operate on delivery_id, not event_id."""
        delivery_id = str(uuid4())

        await outbox.mark_failed(delivery_id, error="Connection error")

        mock_repo.mark_delivery_status.assert_called_once_with(
            delivery_id, status="failed", error="Connection error"
        )

    async def test_mark_skipped_uses_delivery_id(self, outbox: Outbox, mock_repo: AsyncMock):
        """mark_skipped() should operate on delivery_id, not event_id."""
        delivery_id = str(uuid4())

        await outbox.mark_skipped(delivery_id, reason="Backend removed")

        mock_repo.mark_delivery_status.assert_called_once_with(
            delivery_id, status="skipped", error="Backend removed"
        )

    async def test_mark_failed_with_retry_uses_delivery_id(
        self, outbox: Outbox, mock_repo: AsyncMock
    ):
        """mark_failed_with_retry() should operate on delivery_id."""
        delivery_id = str(uuid4())

        await outbox.mark_failed_with_retry(delivery_id, error="Timeout", max_retries=3)

        mock_repo.mark_failed_with_retry.assert_called_once_with(
            delivery_id, error="Timeout", max_retries=3
        )


class TestStaleClaimReset:
    """T007: Test reset_stale_deliveries() resets only stale claims, scoped per consumer group."""

    @pytest.fixture
    def mock_repo(self) -> AsyncMock:
        return AsyncMock()

    @pytest.fixture
    def registry(self) -> SubscriptionRegistry:
        return SubscriptionRegistry({"DummyEvent": {"HandlerA", "HandlerB"}})

    @pytest.fixture
    def outbox(self, mock_repo: AsyncMock, registry: SubscriptionRegistry) -> Outbox:
        outbox = Outbox.__new__(Outbox)
        outbox._repo = mock_repo
        outbox._registry = registry
        return outbox

    async def test_reset_stale_claims_delegates_to_repo(self, outbox: Outbox, mock_repo: AsyncMock):
        """reset_stale_claims() should delegate to reset_stale_deliveries on repo."""
        mock_repo.reset_stale_deliveries.return_value = 3

        count = await outbox.reset_stale_claims(timeout_seconds=300.0)

        assert count == 3
        mock_repo.reset_stale_deliveries.assert_called_once_with(300.0)
