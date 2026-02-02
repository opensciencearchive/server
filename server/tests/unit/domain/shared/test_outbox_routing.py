"""Unit tests for Outbox routing key filtering.

Tests for User Story 4: Event Routing.
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from osa.domain.shared.event import ClaimResult, Event, EventId
from osa.domain.shared.outbox import Outbox


class DummyEvent(Event):
    """Test event for routing tests."""

    id: EventId
    data: str


class TestOutboxRoutingKey:
    """Tests for Outbox claim() with routing_key filtering."""

    @pytest.fixture
    def mock_repo(self) -> AsyncMock:
        """Create a mock EventRepository."""
        return AsyncMock()

    @pytest.fixture
    def outbox(self, mock_repo: AsyncMock) -> Outbox:
        """Create an Outbox with mocked repository."""
        outbox = Outbox.__new__(Outbox)
        outbox._repo = mock_repo
        return outbox

    async def test_claim_with_routing_key_filters_events(
        self, outbox: Outbox, mock_repo: AsyncMock
    ):
        """Outbox.claim() with routing_key should filter to matching events."""
        event = DummyEvent(id=EventId(uuid4()), data="routed")
        mock_repo.claim.return_value = ClaimResult(events=[event], claimed_at=datetime.now(UTC))

        result = await outbox.claim(
            event_types=[DummyEvent],
            limit=10,
            routing_key="vector",
        )

        mock_repo.claim.assert_called_once_with(
            event_types=["DummyEvent"],
            limit=10,
            routing_key="vector",
        )
        assert len(result) == 1

    async def test_claim_with_routing_key_none_matches_unrouted(
        self, outbox: Outbox, mock_repo: AsyncMock
    ):
        """Outbox.claim() with routing_key=None should match unrouted events."""
        event = DummyEvent(id=EventId(uuid4()), data="unrouted")
        mock_repo.claim.return_value = ClaimResult(events=[event], claimed_at=datetime.now(UTC))

        result = await outbox.claim(
            event_types=[DummyEvent],
            limit=10,
            routing_key=None,
        )

        mock_repo.claim.assert_called_once_with(
            event_types=["DummyEvent"],
            limit=10,
            routing_key=None,
        )
        assert len(result) == 1

    async def test_append_with_routing_key(self, outbox: Outbox, mock_repo: AsyncMock):
        """Outbox.append() should pass routing_key to repository."""
        event = DummyEvent(id=EventId(uuid4()), data="routed-event")

        await outbox.append(event, routing_key="keyword")

        mock_repo.save.assert_called_once_with(event, status="pending", routing_key="keyword")

    async def test_append_without_routing_key(self, outbox: Outbox, mock_repo: AsyncMock):
        """Outbox.append() without routing_key should pass None."""
        event = DummyEvent(id=EventId(uuid4()), data="unrouted-event")

        await outbox.append(event)

        mock_repo.save.assert_called_once_with(event, status="pending", routing_key=None)
