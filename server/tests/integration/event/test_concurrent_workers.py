"""Integration tests for concurrent workers claiming different events.

Tests for User Story 2: Concurrent Event Processing.
"""

import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from osa.domain.shared.event import ClaimResult, Delivery, Event, EventId


class DummyEvent(Event):
    """Test event for concurrent worker tests."""

    id: EventId
    data: str


class FakeConcurrentRepository:
    """Fake repository simulating concurrent access with SKIP LOCKED.

    Uses asyncio.Lock to simulate row-level locking behavior.
    """

    def __init__(self):
        self.events: dict[str, dict] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._global_lock = asyncio.Lock()

    async def save(self, event: Event, status: str = "pending"):
        """Save an event."""
        self.events[str(event.id)] = {
            "event": event,
            "status": status,
        }
        self._locks[str(event.id)] = asyncio.Lock()

    async def claim(
        self,
        event_types: list[str],
        limit: int,
    ) -> ClaimResult:
        """Claim events with SKIP LOCKED simulation."""
        deliveries: list[Delivery] = []
        now = datetime.now(UTC)

        async with self._global_lock:
            for event_id, data in self.events.items():
                if len(deliveries) >= limit:
                    break

                # Skip if locked (simulates SKIP LOCKED)
                if self._locks[event_id].locked():
                    continue

                if data["status"] != "pending":
                    continue

                if type(data["event"]).__name__ not in event_types:
                    continue

                # Try to acquire lock
                if self._locks[event_id].locked():
                    continue

                await self._locks[event_id].acquire()
                data["status"] = "claimed"
                deliveries.append(Delivery(id=f"del-{event_id}", event=data["event"]))

        return ClaimResult(deliveries=deliveries, claimed_at=now)

    async def release(self, event_id: str, new_status: str = "delivered"):
        """Release lock and update status."""
        if event_id in self.events:
            self.events[event_id]["status"] = new_status
        if event_id in self._locks and self._locks[event_id].locked():
            self._locks[event_id].release()

    async def reset_stale_claims(self, timeout_seconds: float) -> int:
        """Reset stale claims (stub)."""
        return 0


class TestConcurrentWorkerClaiming:
    """Tests for concurrent workers claiming different events."""

    @pytest.mark.asyncio
    async def test_concurrent_workers_claim_different_events(self):
        """Multiple workers running concurrently should claim different events."""
        # Arrange
        repo = FakeConcurrentRepository()

        # Create 10 events
        events = []
        for i in range(10):
            event = DummyEvent(id=EventId(uuid4()), data=f"event-{i}")
            events.append(event)
            await repo.save(event)

        # Simulate two workers claiming concurrently
        async def worker_claim(worker_id: int, limit: int) -> list[Event]:
            result = await repo.claim(event_types=["DummyEvent"], limit=limit)
            # Simulate some processing time
            await asyncio.sleep(0.01)
            # Release all claimed events
            for event in result.events:
                await repo.release(str(event.id))
            return result.events

        # Act - Run two workers concurrently
        results = await asyncio.gather(
            worker_claim(1, 5),
            worker_claim(2, 5),
        )

        worker1_events = results[0]
        worker2_events = results[1]

        # Assert - Each worker should have claimed some events
        assert len(worker1_events) + len(worker2_events) == 10

        # No overlap - SKIP LOCKED ensures each event claimed by only one worker
        worker1_ids = {e.id for e in worker1_events}
        worker2_ids = {e.id for e in worker2_events}
        assert worker1_ids.isdisjoint(worker2_ids)

    @pytest.mark.asyncio
    async def test_skip_locked_prevents_double_claiming(self):
        """SKIP LOCKED should prevent the same event from being claimed twice."""
        # Arrange
        repo = FakeConcurrentRepository()
        event = DummyEvent(id=EventId(uuid4()), data="single-event")
        await repo.save(event)

        claim_count = 0
        claimed_by = []

        async def try_claim(worker_id: int) -> bool:
            nonlocal claim_count
            result = await repo.claim(event_types=["DummyEvent"], limit=1)
            if result.events:
                claim_count += 1
                claimed_by.append(worker_id)
                # Hold the lock briefly
                await asyncio.sleep(0.02)
                await repo.release(str(result.events[0].id))
                return True
            return False

        # Act - Multiple workers try to claim the same event simultaneously
        results = await asyncio.gather(
            try_claim(1),
            try_claim(2),
            try_claim(3),
        )

        # Assert - Only one worker should have claimed the event
        successful_claims = sum(results)
        assert successful_claims == 1
        assert claim_count == 1

    @pytest.mark.asyncio
    async def test_high_concurrency_no_duplicates(self):
        """Under high concurrency, no events should be processed by multiple workers."""
        # Arrange
        repo = FakeConcurrentRepository()

        # Create 100 events
        for i in range(100):
            event = DummyEvent(id=EventId(uuid4()), data=f"event-{i}")
            await repo.save(event)

        all_claimed_ids: list[set] = []

        async def worker_claim_all(worker_id: int) -> set:
            """Worker keeps claiming until no more events."""
            claimed_ids = set()
            while True:
                result = await repo.claim(event_types=["DummyEvent"], limit=10)
                if not result.events:
                    break
                for event in result.events:
                    claimed_ids.add(event.id)
                    await repo.release(str(event.id))
                await asyncio.sleep(0.001)  # Small delay to simulate processing
            return claimed_ids

        # Act - Run 5 workers concurrently
        results = await asyncio.gather(
            worker_claim_all(1),
            worker_claim_all(2),
            worker_claim_all(3),
            worker_claim_all(4),
            worker_claim_all(5),
        )

        # Collect all claimed IDs
        all_ids = set()
        for claimed_ids in results:
            all_ids.update(claimed_ids)
            all_claimed_ids.append(claimed_ids)

        # Assert - All 100 events should be claimed exactly once
        assert len(all_ids) == 100

        # Check no duplicates across workers
        for i, ids1 in enumerate(all_claimed_ids):
            for j, ids2 in enumerate(all_claimed_ids):
                if i != j:
                    overlap = ids1 & ids2
                    assert len(overlap) == 0, f"Workers {i} and {j} both claimed: {overlap}"
