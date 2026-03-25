"""T015/T017: Unit tests for IngestRun aggregate — status transitions, completion, counters."""

from datetime import UTC, datetime

import pytest

from osa.domain.ingest.model.ingest_run import IngestRun, IngestStatus
from osa.domain.shared.error import InvalidStateError


def _make_run(**overrides) -> IngestRun:
    defaults = {
        "srn": "urn:osa:localhost:ing:test-run",
        "convention_srn": "urn:osa:localhost:conv:test-conv@1.0.0",
        "status": IngestStatus.PENDING,
        "started_at": datetime.now(UTC),
    }
    defaults.update(overrides)
    return IngestRun(**defaults)


class TestStatusTransitions:
    def test_pending_to_running(self) -> None:
        run = _make_run()
        run.mark_running()
        assert run.status == IngestStatus.RUNNING

    def test_running_to_completed(self) -> None:
        run = _make_run(status=IngestStatus.RUNNING)
        run.transition_to(IngestStatus.COMPLETED)
        assert run.status == IngestStatus.COMPLETED

    def test_running_to_failed(self) -> None:
        run = _make_run(status=IngestStatus.RUNNING)
        run.mark_failed(datetime.now(UTC))
        assert run.status == IngestStatus.FAILED
        assert run.completed_at is not None

    def test_pending_to_failed(self) -> None:
        run = _make_run()
        run.mark_failed(datetime.now(UTC))
        assert run.status == IngestStatus.FAILED

    def test_completed_to_running_rejected(self) -> None:
        run = _make_run(status=IngestStatus.COMPLETED)
        with pytest.raises(InvalidStateError, match="Cannot transition"):
            run.transition_to(IngestStatus.RUNNING)

    def test_failed_to_running_rejected(self) -> None:
        run = _make_run(status=IngestStatus.FAILED)
        with pytest.raises(InvalidStateError, match="Cannot transition"):
            run.transition_to(IngestStatus.RUNNING)

    def test_completed_to_completed_rejected(self) -> None:
        run = _make_run(status=IngestStatus.COMPLETED)
        with pytest.raises(InvalidStateError):
            run.transition_to(IngestStatus.COMPLETED)


class TestCompletionCondition:
    def test_not_complete_when_source_not_finished(self) -> None:
        run = _make_run(
            status=IngestStatus.RUNNING,
            source_finished=False,
            batches_sourced=3,
            batches_completed=3,
        )
        assert not run.is_complete

    def test_not_complete_when_batches_pending(self) -> None:
        run = _make_run(
            status=IngestStatus.RUNNING,
            source_finished=True,
            batches_sourced=3,
            batches_completed=2,
        )
        assert not run.is_complete

    def test_complete_when_all_batches_done(self) -> None:
        run = _make_run(
            status=IngestStatus.RUNNING,
            source_finished=True,
            batches_sourced=3,
            batches_completed=3,
        )
        assert run.is_complete

    def test_check_completion_transitions_status(self) -> None:
        run = _make_run(
            status=IngestStatus.RUNNING,
            source_finished=True,
            batches_sourced=2,
            batches_completed=2,
        )
        now = datetime.now(UTC)
        completed = run.check_completion(now)
        assert completed is True
        assert run.status == IngestStatus.COMPLETED
        assert run.completed_at == now

    def test_check_completion_noop_when_not_complete(self) -> None:
        run = _make_run(
            status=IngestStatus.RUNNING,
            source_finished=True,
            batches_sourced=3,
            batches_completed=2,
        )
        completed = run.check_completion(datetime.now(UTC))
        assert completed is False
        assert run.status == IngestStatus.RUNNING


class TestCounterIncrements:
    def test_increment_batches_sourced(self) -> None:
        run = _make_run(status=IngestStatus.RUNNING)
        run.increment_batches_sourced()
        assert run.batches_sourced == 1

    def test_record_batch_completed(self) -> None:
        run = _make_run(status=IngestStatus.RUNNING)
        run.record_batch_completed(published_count=42)
        assert run.batches_completed == 1
        assert run.published_count == 42

    def test_multiple_batch_completions(self) -> None:
        run = _make_run(status=IngestStatus.RUNNING)
        run.record_batch_completed(published_count=100)
        run.record_batch_completed(published_count=50)
        assert run.batches_completed == 2
        assert run.published_count == 150

    def test_mark_source_finished(self) -> None:
        run = _make_run(status=IngestStatus.RUNNING)
        assert not run.source_finished
        run.mark_source_finished()
        assert run.source_finished

    def test_batch_size_default(self) -> None:
        run = _make_run()
        assert run.batch_size == 1000

    def test_custom_batch_size(self) -> None:
        run = _make_run(batch_size=500)
        assert run.batch_size == 500
