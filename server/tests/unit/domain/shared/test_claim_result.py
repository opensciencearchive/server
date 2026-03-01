"""Unit tests for ClaimResult and Delivery value objects.

Tests result of a claim operation.
"""

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from osa.domain.shared.event import ClaimResult, Delivery, Event, EventId


class DummyEvent(Event):
    """Test event for claim result tests."""

    id: EventId
    data: str


class TestDelivery:
    """Tests for the Delivery envelope."""

    def test_create_delivery(self):
        """Delivery should hold a delivery row ID and the event."""
        event = DummyEvent(id=EventId(uuid4()), data="test")
        delivery = Delivery(id="del-123", event=event)

        assert delivery.id == "del-123"
        assert delivery.event is event

    def test_delivery_is_frozen(self):
        """Delivery should be immutable (frozen dataclass)."""
        event = DummyEvent(id=EventId(uuid4()), data="test")
        delivery = Delivery(id="del-123", event=event)

        with pytest.raises(AttributeError):
            delivery.id = "other"  # type: ignore[misc]


class TestClaimResult:
    """Tests for ClaimResult value object."""

    def test_create_with_deliveries(self):
        """ClaimResult should hold claimed deliveries and timestamp."""
        event1 = DummyEvent(id=EventId(uuid4()), data="event1")
        event2 = DummyEvent(id=EventId(uuid4()), data="event2")
        d1 = Delivery(id="del-1", event=event1)
        d2 = Delivery(id="del-2", event=event2)
        now = datetime.now(UTC)

        result = ClaimResult(deliveries=[d1, d2], claimed_at=now)

        assert len(result.deliveries) == 2
        assert result.deliveries[0] is d1
        assert result.deliveries[1] is d2
        assert result.claimed_at == now

    def test_create_empty(self):
        """ClaimResult can be created with empty deliveries list."""
        now = datetime.now(UTC)
        result = ClaimResult(deliveries=[], claimed_at=now)

        assert result.deliveries == []
        assert result.claimed_at == now

    def test_events_property(self):
        """events property should return events from all deliveries."""
        event1 = DummyEvent(id=EventId(uuid4()), data="event1")
        event2 = DummyEvent(id=EventId(uuid4()), data="event2")
        d1 = Delivery(id="del-1", event=event1)
        d2 = Delivery(id="del-2", event=event2)
        now = datetime.now(UTC)

        result = ClaimResult(deliveries=[d1, d2], claimed_at=now)

        assert result.events == [event1, event2]

    def test_events_property_empty(self):
        """events property should return empty list when no deliveries."""
        result = ClaimResult(deliveries=[], claimed_at=datetime.now(UTC))
        assert result.events == []

    def test_immutable(self):
        """ClaimResult should be immutable (frozen dataclass)."""
        now = datetime.now(UTC)
        result = ClaimResult(deliveries=[], claimed_at=now)

        with pytest.raises(AttributeError):
            result.deliveries = []  # type: ignore[misc]

        with pytest.raises(AttributeError):
            result.claimed_at = datetime.now(UTC)  # type: ignore[misc]

    def test_deliveries_required(self):
        """ClaimResult deliveries is required."""
        with pytest.raises(TypeError):
            ClaimResult(claimed_at=datetime.now(UTC))  # type: ignore[call-arg]

    def test_claimed_at_required(self):
        """ClaimResult claimed_at is required."""
        with pytest.raises(TypeError):
            ClaimResult(deliveries=[])  # type: ignore[call-arg]

    def test_bool_true_when_has_deliveries(self):
        """ClaimResult should be truthy when deliveries are present."""
        event = DummyEvent(id=EventId(uuid4()), data="test")
        d = Delivery(id="del-1", event=event)
        result = ClaimResult(deliveries=[d], claimed_at=datetime.now(UTC))
        assert bool(result) is True

    def test_bool_false_when_empty(self):
        """ClaimResult should be falsy when deliveries are empty."""
        result = ClaimResult(deliveries=[], claimed_at=datetime.now(UTC))
        assert bool(result) is False

    def test_len(self):
        """ClaimResult should support len() returning number of deliveries."""
        event1 = DummyEvent(id=EventId(uuid4()), data="event1")
        event2 = DummyEvent(id=EventId(uuid4()), data="event2")
        d1 = Delivery(id="del-1", event=event1)
        d2 = Delivery(id="del-2", event=event2)

        result = ClaimResult(deliveries=[d1, d2], claimed_at=datetime.now(UTC))
        assert len(result) == 2

    def test_iter(self):
        """ClaimResult should be iterable over deliveries."""
        event1 = DummyEvent(id=EventId(uuid4()), data="event1")
        event2 = DummyEvent(id=EventId(uuid4()), data="event2")
        d1 = Delivery(id="del-1", event=event1)
        d2 = Delivery(id="del-2", event=event2)

        result = ClaimResult(deliveries=[d1, d2], claimed_at=datetime.now(UTC))
        deliveries = list(result)
        assert deliveries == [d1, d2]
