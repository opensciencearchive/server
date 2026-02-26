"""Unit tests for Outbox routing key filtering.

Tests for User Story 4: Event Routing.
Updated for consumer-group delivery model.
"""

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from osa.domain.shared.event import Event, EventId
from osa.domain.shared.model.subscription_registry import SubscriptionRegistry
from osa.domain.shared.outbox import Outbox


class DummyEvent(Event):
    """Test event for routing tests."""

    id: EventId
    data: str


class TestOutboxRoutingKey:
    """Tests for Outbox append() with routing_key passthrough."""

    @pytest.fixture
    def mock_repo(self) -> AsyncMock:
        return AsyncMock()

    @pytest.fixture
    def registry(self) -> SubscriptionRegistry:
        return SubscriptionRegistry({"DummyEvent": {"HandlerA"}})

    @pytest.fixture
    def outbox(self, mock_repo: AsyncMock, registry: SubscriptionRegistry) -> Outbox:
        outbox = Outbox.__new__(Outbox)
        outbox._repo = mock_repo
        outbox._registry = registry
        return outbox

    async def test_append_with_routing_key(self, outbox: Outbox, mock_repo: AsyncMock):
        """Outbox.append() should pass routing_key to save_with_deliveries."""
        event = DummyEvent(id=EventId(uuid4()), data="routed-event")

        await outbox.append(event, routing_key="keyword")

        mock_repo.save_with_deliveries.assert_called_once_with(
            event, consumer_groups={"HandlerA"}, routing_key="keyword"
        )

    async def test_append_without_routing_key(self, outbox: Outbox, mock_repo: AsyncMock):
        """Outbox.append() without routing_key should pass None."""
        event = DummyEvent(id=EventId(uuid4()), data="unrouted-event")

        await outbox.append(event)

        mock_repo.save_with_deliveries.assert_called_once_with(
            event, consumer_groups={"HandlerA"}, routing_key=None
        )
