"""Unit tests for WorkerState and WorkerStatus.

Tests runtime state tracking for workers.
"""

from datetime import UTC, datetime

import pytest

from osa.domain.shared.event import (
    Event,
    EventId,
    WorkerConfig,
    WorkerState,
    WorkerStatus,
)


class DummyEvent(Event):
    """Test event for worker state tests."""

    id: EventId
    data: str


class TestWorkerStatus:
    """Tests for WorkerStatus enum."""

    def test_status_values(self):
        """WorkerStatus should have expected values."""
        assert WorkerStatus.IDLE.value == "idle"
        assert WorkerStatus.CLAIMING.value == "claiming"
        assert WorkerStatus.PROCESSING.value == "processing"
        assert WorkerStatus.STOPPING.value == "stopping"

    def test_all_statuses(self):
        """WorkerStatus should have exactly 4 values."""
        assert len(WorkerStatus) == 4


class TestWorkerState:
    """Tests for WorkerState runtime entity."""

    @pytest.fixture
    def config(self) -> WorkerConfig:
        """Fixture providing a valid WorkerConfig."""
        return WorkerConfig(
            name="test-worker",
            event_types=(DummyEvent,),
        )

    def test_initial_state(self, config: WorkerConfig):
        """WorkerState should initialize with idle status and zero counts."""
        state = WorkerState(config=config)
        assert state.config is config
        assert state.status == WorkerStatus.IDLE
        assert state.current_batch == []
        assert state.last_claim_at is None
        assert state.processed_count == 0
        assert state.failed_count == 0
        assert state.error is None

    def test_mutable_status(self, config: WorkerConfig):
        """WorkerState status should be mutable."""
        state = WorkerState(config=config)
        state.status = WorkerStatus.CLAIMING
        assert state.status == WorkerStatus.CLAIMING

    def test_mutable_current_batch(self, config: WorkerConfig):
        """WorkerState current_batch should be mutable."""
        state = WorkerState(config=config)
        # In real usage, events would be appended here
        state.current_batch = ["event1", "event2"]  # type: ignore[list-item]
        assert len(state.current_batch) == 2

    def test_mutable_counters(self, config: WorkerConfig):
        """WorkerState counters should be mutable."""
        state = WorkerState(config=config)
        state.processed_count = 10
        state.failed_count = 2
        assert state.processed_count == 10
        assert state.failed_count == 2

    def test_mutable_last_claim_at(self, config: WorkerConfig):
        """WorkerState last_claim_at should be mutable."""
        state = WorkerState(config=config)
        now = datetime.now(UTC)
        state.last_claim_at = now
        assert state.last_claim_at == now

    def test_mutable_error(self, config: WorkerConfig):
        """WorkerState error should be mutable."""
        state = WorkerState(config=config)
        error = ValueError("test error")
        state.error = error
        assert state.error is error

    def test_state_with_custom_initial_values(self, config: WorkerConfig):
        """WorkerState should accept custom initial values."""
        now = datetime.now(UTC)
        state = WorkerState(
            config=config,
            status=WorkerStatus.PROCESSING,
            current_batch=[],
            last_claim_at=now,
            processed_count=100,
            failed_count=5,
            error=None,
        )
        assert state.status == WorkerStatus.PROCESSING
        assert state.last_claim_at == now
        assert state.processed_count == 100
        assert state.failed_count == 5
