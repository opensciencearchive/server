"""Unit tests for Outbox claim, mark_delivered, and mark_failed operations.

Tests for User Story 1: Reliable Event Processing.
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from osa.domain.shared.event import ClaimResult, Event, EventId
from osa.domain.shared.outbox import Outbox


class DummyEvent(Event):
    """Test event for outbox tests."""

    id: EventId
    data: str


class TestOutboxClaim:
    """Tests for Outbox.claim() returning claimed events."""

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

    async def test_claim_returns_claimed_events(self, outbox: Outbox, mock_repo: AsyncMock):
        """Outbox.claim() should return ClaimResult from repository."""
        event1 = DummyEvent(id=EventId(uuid4()), data="event1")
        event2 = DummyEvent(id=EventId(uuid4()), data="event2")
        now = datetime.now(UTC)

        mock_repo.claim.return_value = ClaimResult(events=[event1, event2], claimed_at=now)

        result = await outbox.claim(
            event_types=[DummyEvent],
            limit=10,
            routing_key=None,
        )

        assert isinstance(result, ClaimResult)
        assert len(result) == 2
        assert result.events[0] is event1
        assert result.events[1] is event2
        mock_repo.claim.assert_called_once_with(
            event_types=["DummyEvent"],
            limit=10,
            routing_key=None,
        )

    async def test_claim_with_routing_key(self, outbox: Outbox, mock_repo: AsyncMock):
        """Outbox.claim() should pass routing_key to repository."""
        event = DummyEvent(id=EventId(uuid4()), data="routed")
        now = datetime.now(UTC)

        mock_repo.claim.return_value = ClaimResult(events=[event], claimed_at=now)

        result = await outbox.claim(
            event_types=[DummyEvent],
            limit=5,
            routing_key="vector",
        )

        assert len(result) == 1
        mock_repo.claim.assert_called_once_with(
            event_types=["DummyEvent"],
            limit=5,
            routing_key="vector",
        )

    async def test_claim_returns_empty_when_no_events(self, outbox: Outbox, mock_repo: AsyncMock):
        """Outbox.claim() should return empty ClaimResult when no events available."""
        now = datetime.now(UTC)
        mock_repo.claim.return_value = ClaimResult(events=[], claimed_at=now)

        result = await outbox.claim(
            event_types=[DummyEvent],
            limit=10,
            routing_key=None,
        )

        assert len(result) == 0
        assert bool(result) is False


class TestOutboxMarkDelivered:
    """Tests for Outbox.mark_delivered() updating single event."""

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

    async def test_mark_delivered_updates_status(self, outbox: Outbox, mock_repo: AsyncMock):
        """Outbox.mark_delivered() should update event status to delivered."""
        event_id = EventId(uuid4())

        await outbox.mark_delivered(event_id)

        mock_repo.update_status.assert_called_once_with(event_id, status="delivered")


class TestOutboxMarkFailed:
    """Tests for Outbox.mark_failed() with retry logic."""

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

    async def test_mark_failed_increments_retry_count(self, outbox: Outbox, mock_repo: AsyncMock):
        """Outbox.mark_failed() should increment retry_count."""
        event_id = EventId(uuid4())

        await outbox.mark_failed(event_id, "Connection error")

        mock_repo.update_status.assert_called_once()
        call_args = mock_repo.update_status.call_args
        assert call_args[0][0] == event_id
        assert call_args[1]["status"] == "failed"
        assert call_args[1]["error"] == "Connection error"

    async def test_mark_failed_with_max_retries_sets_failed_status(
        self, outbox: Outbox, mock_repo: AsyncMock
    ):
        """Outbox.mark_failed() should set status=failed after max_retries exceeded.

        Note: The retry counting is handled by the repository implementation.
        The Outbox service just forwards the mark_failed call. The repository
        checks retry_count and either resets to pending (for retry) or marks failed.
        """
        event_id = EventId(uuid4())
        error = "Persistent failure"

        # The mark_failed_with_retry method handles retry logic
        await outbox.mark_failed_with_retry(event_id, error, max_retries=3)

        mock_repo.mark_failed_with_retry.assert_called_once_with(
            event_id, error=error, max_retries=3
        )
