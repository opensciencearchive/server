"""Unit tests for WorkerConfig value object.

Tests validation rules for worker configuration.
"""

import pytest
from pydantic import ValidationError

from osa.domain.shared.event import Event, EventId, WorkerConfig


class DummyEvent(Event):
    """Test event for worker config tests."""

    id: EventId
    data: str


class TestWorkerConfigValidation:
    """Tests for WorkerConfig validation rules."""

    def test_valid_config(self):
        """WorkerConfig with valid values should be created."""
        config = WorkerConfig(
            name="test-worker",
            event_types=(DummyEvent,),
            routing_key="test-key",
            batch_size=10,
            batch_timeout=5.0,
            poll_interval=0.5,
            max_retries=3,
            claim_timeout=300.0,
        )
        assert config.name == "test-worker"
        assert config.event_types == (DummyEvent,)
        assert config.routing_key == "test-key"
        assert config.batch_size == 10
        assert config.batch_timeout == 5.0
        assert config.poll_interval == 0.5
        assert config.max_retries == 3
        assert config.claim_timeout == 300.0

    def test_defaults(self):
        """WorkerConfig should have sensible defaults."""
        config = WorkerConfig(
            name="test-worker",
            event_types=(DummyEvent,),
        )
        assert config.routing_key is None
        assert config.batch_size == 1
        assert config.batch_timeout == 5.0
        assert config.poll_interval == 0.5
        assert config.max_retries == 3
        assert config.claim_timeout == 300.0

    def test_name_required(self):
        """WorkerConfig name is required."""
        with pytest.raises(ValidationError):
            WorkerConfig(event_types=(DummyEvent,))  # type: ignore[call-arg]

    def test_event_types_required(self):
        """WorkerConfig event_types is required."""
        with pytest.raises(ValidationError):
            WorkerConfig(name="test-worker")  # type: ignore[call-arg]

    def test_event_types_not_empty(self):
        """WorkerConfig event_types must not be empty."""
        with pytest.raises(ValidationError, match="event_types must not be empty"):
            WorkerConfig(name="test-worker", event_types=())

    def test_batch_size_must_be_positive(self):
        """WorkerConfig batch_size must be >= 1."""
        with pytest.raises(ValidationError, match="greater than or equal to 1"):
            WorkerConfig(name="test-worker", event_types=(DummyEvent,), batch_size=0)

    def test_batch_timeout_must_be_positive(self):
        """WorkerConfig batch_timeout must be > 0."""
        with pytest.raises(ValidationError, match="greater than 0"):
            WorkerConfig(name="test-worker", event_types=(DummyEvent,), batch_timeout=0)

    def test_poll_interval_must_be_positive(self):
        """WorkerConfig poll_interval must be > 0."""
        with pytest.raises(ValidationError, match="greater than 0"):
            WorkerConfig(name="test-worker", event_types=(DummyEvent,), poll_interval=0)

    def test_max_retries_must_be_non_negative(self):
        """WorkerConfig max_retries must be >= 0."""
        with pytest.raises(ValidationError, match="greater than or equal to 0"):
            WorkerConfig(name="test-worker", event_types=(DummyEvent,), max_retries=-1)

    def test_claim_timeout_must_be_greater_than_batch_timeout(self):
        """WorkerConfig claim_timeout must be > batch_timeout."""
        with pytest.raises(ValidationError, match="claim_timeout must be > batch_timeout"):
            WorkerConfig(
                name="test-worker",
                event_types=(DummyEvent,),
                batch_timeout=10.0,
                claim_timeout=5.0,
            )

    def test_immutable(self):
        """WorkerConfig should be immutable (frozen Pydantic model)."""
        config = WorkerConfig(name="test-worker", event_types=(DummyEvent,))
        with pytest.raises(ValidationError, match="frozen"):
            config.name = "new-name"  # type: ignore[misc]
