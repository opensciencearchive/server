"""Unit tests for HookRun — pure execution record + per-row provenance anchor (#145).

Post design-revision: HookRun carries no execution-context columns (no
``ingest_run_id`` / ``deposition_id`` / ``batch_index`` / XOR check). Data origin
is reached via the feature row's ``record_srn → records.source``; this record is
purely *what code ran, when, and where the logs are*.
"""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from osa.domain.validation.model.hook_release import HookReleaseId
from osa.domain.validation.model.hook_result import HookStatus
from osa.domain.validation.model.hook_run import HookRun, HookRunId, HookRunStatus


def _run(**overrides: object) -> HookRun:
    started = datetime.now(UTC)
    base: dict[str, object] = dict(
        id=HookRunId(uuid4()),
        release_id=HookReleaseId(uuid4()),
        status=HookRunStatus.PASSED,
        started_at=started,
        finished_at=started + timedelta(seconds=2),
        duration_s=2.0,
        oom_retries=0,
    )
    base.update(overrides)
    return HookRun(**base)  # type: ignore[arg-type]


def test_status_enum_values() -> None:
    assert {s.value for s in HookRunStatus} == {"passed", "warnings", "failed", "error"}


def test_pure_execution_record() -> None:
    run = _run()
    assert run.duration_s == 2.0
    assert run.oom_retries == 0
    assert run.log_ref is None
    # No execution-context columns remain on the pure record.
    assert not hasattr(run, "ingest_run_id")
    assert not hasattr(run, "deposition_id")
    assert not hasattr(run, "batch_index")


def test_log_ref_is_optional() -> None:
    run = _run(log_ref="s3://logs/run-1.txt")
    assert run.log_ref == "s3://logs/run-1.txt"


def test_finished_at_and_duration_required() -> None:
    with pytest.raises(ValueError):
        HookRun(
            id=HookRunId(uuid4()),
            release_id=HookReleaseId(uuid4()),
            status=HookRunStatus.PASSED,
            started_at=datetime.now(UTC),
        )  # type: ignore[call-arg]


def test_from_hook_status_maps_passed_and_rejected() -> None:
    assert HookRunStatus.from_hook_status(HookStatus.PASSED) is HookRunStatus.PASSED
    assert HookRunStatus.from_hook_status(HookStatus.REJECTED) is HookRunStatus.FAILED
