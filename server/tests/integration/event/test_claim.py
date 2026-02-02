"""Integration tests for event claiming with FOR UPDATE SKIP LOCKED.

Tests for User Story 1: Reliable Event Processing.
"""

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from osa.domain.shared.event import ClaimResult, Event, EventId


class DummyEvent(Event):
    """Test event for claim tests."""

    id: EventId
    data: str


class FakeEventRepository:
    """Fake event repository simulating FOR UPDATE SKIP LOCKED behavior.

    Tracks claimed events and ensures concurrent claims skip locked rows.
    """

    def __init__(self):
        self.events: dict[str, dict[str, Any]] = {}
        self._locked_ids: set[str] = set()

    async def save(
        self, event: Event, status: str = "pending", routing_key: str | None = None
    ) -> None:
        """Save an event."""
        self.events[str(event.id)] = {
            "event": event,
            "status": status,
            "routing_key": routing_key,
            "retry_count": 0,
            "claimed_at": None,
            "updated_at": datetime.now(UTC),
        }

    async def claim(
        self,
        event_types: list[str],
        limit: int,
        routing_key: str | None = None,
    ) -> ClaimResult:
        """Claim events, simulating FOR UPDATE SKIP LOCKED."""
        claimed = []
        now = datetime.now(UTC)

        for event_id, data in self.events.items():
            if len(claimed) >= limit:
                break

            # Skip already locked events (simulates SKIP LOCKED)
            if event_id in self._locked_ids:
                continue

            # Check status
            if data["status"] != "pending":
                continue

            # Check event type
            if type(data["event"]).__name__ not in event_types:
                continue

            # Check routing key
            if routing_key is not None and data["routing_key"] != routing_key:
                continue

            # Lock and claim
            self._locked_ids.add(event_id)
            data["status"] = "claimed"
            data["claimed_at"] = now
            claimed.append(data["event"])

        return ClaimResult(events=claimed, claimed_at=now)

    async def update_status(
        self,
        event_id: EventId,
        status: str,
        error: str | None = None,
    ) -> None:
        """Update event status and release lock."""
        event_id_str = str(event_id)
        if event_id_str in self.events:
            self.events[event_id_str]["status"] = status
            self.events[event_id_str]["updated_at"] = datetime.now(UTC)
            if error:
                self.events[event_id_str]["delivery_error"] = error
            # Release lock when delivered/failed
            if status in ("delivered", "failed", "skipped"):
                self._locked_ids.discard(event_id_str)

    async def mark_failed_with_retry(
        self,
        event_id: EventId,
        error: str,
        max_retries: int,
    ) -> None:
        """Mark failed with retry logic."""
        event_id_str = str(event_id)
        if event_id_str not in self.events:
            return

        data = self.events[event_id_str]
        data["retry_count"] += 1
        data["updated_at"] = datetime.now(UTC)

        if data["retry_count"] >= max_retries:
            data["status"] = "failed"
            data["delivery_error"] = error
        else:
            # Reset to pending for retry
            data["status"] = "pending"
            data["claimed_at"] = None

        self._locked_ids.discard(event_id_str)

    def release_lock(self, event_id: str) -> None:
        """Release lock (simulates transaction rollback)."""
        self._locked_ids.discard(event_id)


class TestClaimWithSkipLocked:
    """Tests for FOR UPDATE SKIP LOCKED behavior."""

    @pytest.fixture
    def repo(self) -> FakeEventRepository:
        """Create a fake event repository."""
        return FakeEventRepository()

    @pytest.mark.asyncio
    async def test_claim_returns_pending_events(self, repo: FakeEventRepository):
        """Claim should return pending events matching event_types."""
        # Arrange
        event1 = DummyEvent(id=EventId(uuid4()), data="event1")
        event2 = DummyEvent(id=EventId(uuid4()), data="event2")
        await repo.save(event1)
        await repo.save(event2)

        # Act
        result = await repo.claim(event_types=["DummyEvent"], limit=10)

        # Assert
        assert len(result) == 2
        assert result.events[0].data in ("event1", "event2")
        assert result.events[1].data in ("event1", "event2")

    @pytest.mark.asyncio
    async def test_claim_respects_limit(self, repo: FakeEventRepository):
        """Claim should respect the limit parameter."""
        # Arrange
        for i in range(10):
            await repo.save(DummyEvent(id=EventId(uuid4()), data=f"event{i}"))

        # Act
        result = await repo.claim(event_types=["DummyEvent"], limit=3)

        # Assert
        assert len(result) == 3

    @pytest.mark.asyncio
    async def test_claim_skips_already_claimed_events(self, repo: FakeEventRepository):
        """Concurrent claims should skip already locked events."""
        # Arrange
        events = [DummyEvent(id=EventId(uuid4()), data=f"event{i}") for i in range(5)]
        for event in events:
            await repo.save(event)

        # Act - First worker claims some events
        result1 = await repo.claim(event_types=["DummyEvent"], limit=3)
        # Second worker tries to claim - should skip locked ones
        result2 = await repo.claim(event_types=["DummyEvent"], limit=3)

        # Assert
        assert len(result1) == 3
        assert len(result2) == 2  # Only remaining unclaimed events

        # Verify no overlap
        ids1 = {e.id for e in result1.events}
        ids2 = {e.id for e in result2.events}
        assert ids1.isdisjoint(ids2)

    @pytest.mark.asyncio
    async def test_claim_filters_by_routing_key(self, repo: FakeEventRepository):
        """Claim should filter by routing_key when specified."""
        # Arrange
        event1 = DummyEvent(id=EventId(uuid4()), data="vector-event")
        event2 = DummyEvent(id=EventId(uuid4()), data="keyword-event")
        event3 = DummyEvent(id=EventId(uuid4()), data="unrouted-event")

        await repo.save(event1, routing_key="vector")
        await repo.save(event2, routing_key="keyword")
        await repo.save(event3, routing_key=None)

        # Act
        vector_result = await repo.claim(event_types=["DummyEvent"], limit=10, routing_key="vector")

        # Assert
        assert len(vector_result) == 1
        assert vector_result.events[0].data == "vector-event"

    @pytest.mark.asyncio
    async def test_claim_sets_status_to_claimed(self, repo: FakeEventRepository):
        """Claim should set event status to 'claimed'."""
        # Arrange
        event = DummyEvent(id=EventId(uuid4()), data="test")
        await repo.save(event)

        # Act
        await repo.claim(event_types=["DummyEvent"], limit=1)

        # Assert
        assert repo.events[str(event.id)]["status"] == "claimed"

    @pytest.mark.asyncio
    async def test_claim_sets_claimed_at_timestamp(self, repo: FakeEventRepository):
        """Claim should set claimed_at timestamp."""
        # Arrange
        event = DummyEvent(id=EventId(uuid4()), data="test")
        await repo.save(event)

        # Act
        before = datetime.now(UTC)
        result = await repo.claim(event_types=["DummyEvent"], limit=1)
        after = datetime.now(UTC)

        # Assert
        claimed_at = repo.events[str(event.id)]["claimed_at"]
        assert claimed_at is not None
        assert before <= claimed_at <= after
        assert result.claimed_at == claimed_at


class TestPartialFailureRecovery:
    """Tests for partial failure recovery."""

    @pytest.fixture
    def repo(self) -> FakeEventRepository:
        """Create a fake event repository."""
        return FakeEventRepository()

    @pytest.mark.asyncio
    async def test_mark_delivered_releases_lock(self, repo: FakeEventRepository):
        """mark_delivered should release the lock and set status to delivered."""
        # Arrange
        event = DummyEvent(id=EventId(uuid4()), data="test")
        await repo.save(event)
        await repo.claim(event_types=["DummyEvent"], limit=1)

        # Act
        await repo.update_status(event.id, status="delivered")

        # Assert
        assert repo.events[str(event.id)]["status"] == "delivered"
        assert str(event.id) not in repo._locked_ids

    @pytest.mark.asyncio
    async def test_mark_failed_with_retry_resets_to_pending(self, repo: FakeEventRepository):
        """mark_failed_with_retry should reset to pending if retries remain."""
        # Arrange
        event = DummyEvent(id=EventId(uuid4()), data="test")
        await repo.save(event)
        await repo.claim(event_types=["DummyEvent"], limit=1)

        # Act - First failure (retry_count becomes 1, max is 3)
        await repo.mark_failed_with_retry(event.id, "Error 1", max_retries=3)

        # Assert - Should be pending for retry
        assert repo.events[str(event.id)]["status"] == "pending"
        assert repo.events[str(event.id)]["retry_count"] == 1
        assert str(event.id) not in repo._locked_ids

    @pytest.mark.asyncio
    async def test_mark_failed_after_max_retries_sets_failed(self, repo: FakeEventRepository):
        """mark_failed_with_retry should set status=failed after max_retries."""
        # Arrange
        event = DummyEvent(id=EventId(uuid4()), data="test")
        await repo.save(event)

        # Simulate 3 failures (max_retries=3)
        for i in range(3):
            await repo.claim(event_types=["DummyEvent"], limit=1)
            await repo.mark_failed_with_retry(event.id, f"Error {i + 1}", max_retries=3)

        # Assert - After 3 retries, should be failed
        assert repo.events[str(event.id)]["status"] == "failed"
        assert repo.events[str(event.id)]["retry_count"] == 3

    @pytest.mark.asyncio
    async def test_partial_batch_some_succeed_some_fail(self, repo: FakeEventRepository):
        """In a batch, some events can succeed while others fail."""
        # Arrange
        events = [DummyEvent(id=EventId(uuid4()), data=f"event{i}") for i in range(5)]
        for event in events:
            await repo.save(event)

        # Claim all events
        await repo.claim(event_types=["DummyEvent"], limit=5)

        # Act - Mark first 3 as delivered, last 2 as failed
        for event in events[:3]:
            await repo.update_status(event.id, status="delivered")
        for event in events[3:]:
            await repo.mark_failed_with_retry(event.id, "Processing error", max_retries=3)

        # Assert
        delivered = [e for e in events if repo.events[str(e.id)]["status"] == "delivered"]
        pending = [e for e in events if repo.events[str(e.id)]["status"] == "pending"]

        assert len(delivered) == 3
        assert len(pending) == 2  # Failed ones reset to pending for retry

    @pytest.mark.asyncio
    async def test_released_events_can_be_reclaimed(self, repo: FakeEventRepository):
        """Events released (via rollback or retry) can be claimed by other workers."""
        # Arrange
        event = DummyEvent(id=EventId(uuid4()), data="test")
        await repo.save(event)

        # First worker claims
        await repo.claim(event_types=["DummyEvent"], limit=1)
        assert repo.events[str(event.id)]["status"] == "claimed"

        # First worker fails and releases
        await repo.mark_failed_with_retry(event.id, "Error", max_retries=3)

        # Act - Second worker can now claim
        result = await repo.claim(event_types=["DummyEvent"], limit=1)

        # Assert
        assert len(result) == 1
        assert result.events[0].id == event.id
