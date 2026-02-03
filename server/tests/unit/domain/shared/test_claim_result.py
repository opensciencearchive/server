"""Unit tests for ClaimResult value object.

Tests result of a claim operation.
"""

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from osa.domain.shared.event import ClaimResult, Event, EventId


class DummyEvent(Event):
    """Test event for claim result tests."""

    id: EventId
    data: str


class TestClaimResult:
    """Tests for ClaimResult value object."""

    def test_create_with_events(self):
        """ClaimResult should hold claimed events and timestamp."""
        event1 = DummyEvent(id=EventId(uuid4()), data="event1")
        event2 = DummyEvent(id=EventId(uuid4()), data="event2")
        now = datetime.now(UTC)

        result = ClaimResult(events=[event1, event2], claimed_at=now)

        assert len(result.events) == 2
        assert result.events[0] is event1
        assert result.events[1] is event2
        assert result.claimed_at == now

    def test_create_empty(self):
        """ClaimResult can be created with empty events list."""
        now = datetime.now(UTC)
        result = ClaimResult(events=[], claimed_at=now)

        assert result.events == []
        assert result.claimed_at == now

    def test_immutable(self):
        """ClaimResult should be immutable (frozen dataclass)."""
        now = datetime.now(UTC)
        result = ClaimResult(events=[], claimed_at=now)

        with pytest.raises(AttributeError):
            result.events = []  # type: ignore[misc]

        with pytest.raises(AttributeError):
            result.claimed_at = datetime.now(UTC)  # type: ignore[misc]

    def test_events_required(self):
        """ClaimResult events is required."""
        with pytest.raises(TypeError):
            ClaimResult(claimed_at=datetime.now(UTC))  # type: ignore[call-arg]

    def test_claimed_at_required(self):
        """ClaimResult claimed_at is required."""
        with pytest.raises(TypeError):
            ClaimResult(events=[])  # type: ignore[call-arg]

    def test_bool_true_when_has_events(self):
        """ClaimResult should be truthy when events are present."""
        event = DummyEvent(id=EventId(uuid4()), data="test")
        result = ClaimResult(events=[event], claimed_at=datetime.now(UTC))
        assert bool(result) is True

    def test_bool_false_when_empty(self):
        """ClaimResult should be falsy when events are empty."""
        result = ClaimResult(events=[], claimed_at=datetime.now(UTC))
        assert bool(result) is False

    def test_len(self):
        """ClaimResult should support len() returning number of events."""
        event1 = DummyEvent(id=EventId(uuid4()), data="event1")
        event2 = DummyEvent(id=EventId(uuid4()), data="event2")

        result = ClaimResult(events=[event1, event2], claimed_at=datetime.now(UTC))
        assert len(result) == 2

    def test_iter(self):
        """ClaimResult should be iterable over events."""
        event1 = DummyEvent(id=EventId(uuid4()), data="event1")
        event2 = DummyEvent(id=EventId(uuid4()), data="event2")

        result = ClaimResult(events=[event1, event2], claimed_at=datetime.now(UTC))
        events = list(result)
        assert events == [event1, event2]
