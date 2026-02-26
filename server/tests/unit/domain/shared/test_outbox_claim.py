"""Unit tests for Outbox claim, mark_delivered, and mark_failed operations.

Tests for User Story 1: Reliable Event Processing.
Updated for consumer-group delivery model.
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from osa.domain.shared.event import ClaimResult, Event, EventId
from osa.domain.shared.model.subscription_registry import SubscriptionRegistry
from osa.domain.shared.outbox import Outbox


class DummyEvent(Event):
    """Test event for outbox tests."""

    id: EventId
    data: str


class TestOutboxClaim:
    """Tests for Outbox.claim() returning claimed events."""

    @pytest.fixture
    def mock_repo(self) -> AsyncMock:
        return AsyncMock()

    @pytest.fixture
    def registry(self) -> SubscriptionRegistry:
        return SubscriptionRegistry({"DummyEvent": {"TestHandler"}})

    @pytest.fixture
    def outbox(self, mock_repo: AsyncMock, registry: SubscriptionRegistry) -> Outbox:
        outbox = Outbox.__new__(Outbox)
        outbox._repo = mock_repo
        outbox._registry = registry
        return outbox

    async def test_claim_returns_claimed_events(self, outbox: Outbox, mock_repo: AsyncMock):
        """Outbox.claim() should return ClaimResult from repository."""
        event1 = DummyEvent(id=EventId(uuid4()), data="event1")
        event2 = DummyEvent(id=EventId(uuid4()), data="event2")
        now = datetime.now(UTC)

        mock_repo.claim_delivery.return_value = ClaimResult(events=[event1, event2], claimed_at=now)

        result = await outbox.claim(
            event_types=[DummyEvent],
            limit=10,
            consumer_group="TestHandler",
        )

        assert isinstance(result, ClaimResult)
        assert len(result) == 2
        assert result.events[0] is event1
        assert result.events[1] is event2
        mock_repo.claim_delivery.assert_called_once_with(
            consumer_group="TestHandler",
            event_types=["DummyEvent"],
            limit=10,
        )

    async def test_claim_returns_empty_when_no_events(self, outbox: Outbox, mock_repo: AsyncMock):
        """Outbox.claim() should return empty ClaimResult when no events available."""
        now = datetime.now(UTC)
        mock_repo.claim_delivery.return_value = ClaimResult(events=[], claimed_at=now)

        result = await outbox.claim(
            event_types=[DummyEvent],
            limit=10,
            consumer_group="TestHandler",
        )

        assert len(result) == 0
        assert bool(result) is False


class TestOutboxMarkDelivered:
    """Tests for Outbox.mark_delivered() using delivery IDs."""

    @pytest.fixture
    def mock_repo(self) -> AsyncMock:
        return AsyncMock()

    @pytest.fixture
    def registry(self) -> SubscriptionRegistry:
        return SubscriptionRegistry({"DummyEvent": {"TestHandler"}})

    @pytest.fixture
    def outbox(self, mock_repo: AsyncMock, registry: SubscriptionRegistry) -> Outbox:
        outbox = Outbox.__new__(Outbox)
        outbox._repo = mock_repo
        outbox._registry = registry
        return outbox

    async def test_mark_delivered_updates_status(self, outbox: Outbox, mock_repo: AsyncMock):
        """Outbox.mark_delivered() should update delivery status to delivered."""
        delivery_id = str(uuid4())

        await outbox.mark_delivered(delivery_id)

        mock_repo.mark_delivery_status.assert_called_once_with(delivery_id, status="delivered")


class TestOutboxMarkFailed:
    """Tests for Outbox.mark_failed() with retry logic."""

    @pytest.fixture
    def mock_repo(self) -> AsyncMock:
        return AsyncMock()

    @pytest.fixture
    def registry(self) -> SubscriptionRegistry:
        return SubscriptionRegistry({"DummyEvent": {"TestHandler"}})

    @pytest.fixture
    def outbox(self, mock_repo: AsyncMock, registry: SubscriptionRegistry) -> Outbox:
        outbox = Outbox.__new__(Outbox)
        outbox._repo = mock_repo
        outbox._registry = registry
        return outbox

    async def test_mark_failed_updates_status(self, outbox: Outbox, mock_repo: AsyncMock):
        """Outbox.mark_failed() should update delivery status to failed."""
        delivery_id = str(uuid4())

        await outbox.mark_failed(delivery_id, "Connection error")

        mock_repo.mark_delivery_status.assert_called_once_with(
            delivery_id, status="failed", error="Connection error"
        )

    async def test_mark_failed_with_retry_delegates_to_repo(
        self, outbox: Outbox, mock_repo: AsyncMock
    ):
        """Outbox.mark_failed_with_retry() should delegate to repo."""
        delivery_id = str(uuid4())
        error = "Persistent failure"

        await outbox.mark_failed_with_retry(delivery_id, error, max_retries=3)

        mock_repo.mark_failed_with_retry.assert_called_once_with(
            delivery_id, error=error, max_retries=3
        )
