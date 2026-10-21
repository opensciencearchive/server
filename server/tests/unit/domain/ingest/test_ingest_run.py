"""T015/T017: Unit tests for IngestRun aggregate — status transitions, completion, counters."""

from datetime import UTC, datetime

import pytest
from uuid import uuid4

from osa.domain.ingest.model.ingester_release import IngesterReleaseId
from osa.domain.ingest.model.ingest_run import IngestRun, IngestStatus
from osa.domain.shared.error import InvalidStateError
from osa.domain.shared.failure import FailureKind


def _make_run(**overrides) -> IngestRun:
    defaults = {
        "id": "test-run-id",
        "convention_id": "urn:osa:localhost:conv:test-conv@1.0.0",
        "release_id": IngesterReleaseId(uuid4()),
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
        run.mark_failed(datetime.now(UTC), reason="image pull failed", kind=FailureKind.IMAGE_PULL)
        assert run.status == IngestStatus.FAILED
        assert run.completed_at is not None
        # The queryable explanation (#152): a failed run says why.
        assert run.failure_reason == "image pull failed"
        assert run.failure_kind is FailureKind.IMAGE_PULL
        # No more batches will be scheduled for a failed run.
        assert run.ingestion_finished is True

    def test_pending_to_failed(self) -> None:
        run = _make_run()
        run.mark_failed(datetime.now(UTC), reason="rbac denied", kind=FailureKind.RBAC)
        assert run.status == IngestStatus.FAILED

    def test_failure_fields_default_to_none(self) -> None:
        run = _make_run()
        assert run.failure_reason is None
        assert run.failure_kind is None

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
            ingestion_finished=False,
            batches_ingested=3,
            batches_completed=3,
        )
        assert not run.is_complete

    def test_not_complete_when_batches_pending(self) -> None:
        run = _make_run(
            status=IngestStatus.RUNNING,
            ingestion_finished=True,
            batches_ingested=3,
            batches_completed=2,
        )
        assert not run.is_complete

    def test_complete_when_all_batches_done(self) -> None:
        run = _make_run(
            status=IngestStatus.RUNNING,
            ingestion_finished=True,
            batches_ingested=3,
            batches_completed=3,
        )
        assert run.is_complete

    def test_check_completion_transitions_status(self) -> None:
        run = _make_run(
            status=IngestStatus.RUNNING,
            ingestion_finished=True,
            batches_ingested=2,
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
            ingestion_finished=True,
            batches_ingested=3,
            batches_completed=2,
        )
        completed = run.check_completion(datetime.now(UTC))
        assert completed is False
        assert run.status == IngestStatus.RUNNING


class TestCounterIncrements:
    def test_increment_batches_ingested(self) -> None:
        run = _make_run(status=IngestStatus.RUNNING)
        run.increment_batches_ingested()
        assert run.batches_ingested == 1

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

    def test_mark_ingestion_finished(self) -> None:
        run = _make_run(status=IngestStatus.RUNNING)
        assert not run.ingestion_finished
        run.mark_ingestion_finished()
        assert run.ingestion_finished

    def test_batch_size_default(self) -> None:
        run = _make_run()
        assert run.batch_size == 1000

    def test_custom_batch_size(self) -> None:
        run = _make_run(batch_size=500)
        assert run.batch_size == 500


class TestBatchFailureAccounting:
    def test_batches_failed_defaults_to_zero(self) -> None:
        run = _make_run()
        assert run.batches_failed == 0

    def test_complete_with_all_batches_succeeded(self) -> None:
        run = _make_run(
            status=IngestStatus.RUNNING,
            ingestion_finished=True,
            batches_ingested=3,
            batches_completed=3,
            batches_failed=0,
        )
        assert run.is_complete

    def test_complete_with_some_batches_failed(self) -> None:
        """A run completes when all batches are accounted for, even if some failed."""
        run = _make_run(
            status=IngestStatus.RUNNING,
            ingestion_finished=True,
            batches_ingested=3,
            batches_completed=2,
            batches_failed=1,
        )
        assert run.is_complete

    def test_complete_with_all_batches_failed(self) -> None:
        run = _make_run(
            status=IngestStatus.RUNNING,
            ingestion_finished=True,
            batches_ingested=3,
            batches_completed=0,
            batches_failed=3,
        )
        assert run.is_complete

    def test_not_complete_when_batches_still_pending(self) -> None:
        run = _make_run(
            status=IngestStatus.RUNNING,
            ingestion_finished=True,
            batches_ingested=3,
            batches_completed=1,
            batches_failed=1,
        )
        assert not run.is_complete

    def test_not_complete_when_ingestion_not_finished(self) -> None:
        run = _make_run(
            status=IngestStatus.RUNNING,
            ingestion_finished=False,
            batches_ingested=3,
            batches_completed=0,
            batches_failed=3,
        )
        assert not run.is_complete

    def test_check_completion_transitions_with_failures(self) -> None:
        run = _make_run(
            status=IngestStatus.RUNNING,
            ingestion_finished=True,
            batches_ingested=3,
            batches_completed=2,
            batches_failed=1,
        )
        now = datetime.now(UTC)
        completed = run.check_completion(now)
        assert completed is True
        assert run.status == IngestStatus.COMPLETED
        assert run.completed_at == now


class TestRecordLimitArithmetic:
    """The run limit counts records actually sourced, not batches times batch_size.

    The old estimate assumed every batch filled, and batches routinely don't:
    the SDK entrypoint silently skips records whose files exceed max_file_mb,
    and the last batch of a limited run is capped short. So the estimate ran
    ahead of reality and the run stopped early — `--limit 250` could yield 170
    records with nothing in the log to explain the shortfall (#227).
    """

    def test_records_sourced_defaults_to_zero(self) -> None:
        assert _make_run().records_sourced == 0

    def test_unlimited_run_always_asks_for_a_full_batch(self) -> None:
        run = _make_run(batch_size=100, limit=None, records_sourced=5000)
        assert run.next_batch_limit() == 100

    def test_limit_caps_the_final_batch(self) -> None:
        run = _make_run(batch_size=100, limit=250, records_sourced=200)
        assert run.next_batch_limit() == 50

    def test_underfilled_batches_do_not_shrink_the_next_batch(self) -> None:
        # Three batches of 60 against a 250 limit: 180 sourced, so the next
        # batch may still be a full 70. The batch-count estimate would have
        # said 300 sourced and stopped the run.
        run = _make_run(batch_size=100, limit=250, batches_ingested=3, records_sourced=180)
        assert run.next_batch_limit() == 70

    def test_limit_not_reached_while_underfilled(self) -> None:
        run = _make_run(batch_size=100, limit=250, batches_ingested=3, records_sourced=180)
        assert run.limit_reached is False

    def test_limit_reached_on_exact_count(self) -> None:
        run = _make_run(batch_size=100, limit=250, records_sourced=250)
        assert run.limit_reached is True

    def test_limit_reached_when_overshot(self) -> None:
        run = _make_run(batch_size=100, limit=250, records_sourced=260)
        assert run.limit_reached is True

    def test_unlimited_run_never_reaches_its_limit(self) -> None:
        assert _make_run(limit=None, records_sourced=10**6).limit_reached is False

    def test_limit_met_after_counts_the_batch_in_hand(self) -> None:
        run = _make_run(batch_size=100, limit=250, records_sourced=180)
        assert run.limit_met_after(70) is True
        assert run.limit_met_after(60) is False

    def test_unlimited_run_is_never_met(self) -> None:
        assert _make_run(limit=None, records_sourced=0).limit_met_after(10**6) is False

    def test_exhausted_limit_asks_for_nothing(self) -> None:
        run = _make_run(batch_size=100, limit=250, records_sourced=250)
        assert run.next_batch_limit() == 0
