"""Unit tests for HookRun (append-only execution record + provenance anchor)."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from osa.domain.ingest.model.ingest_run import IngestRunId
from osa.domain.shared.model.srn import DepositionSRN, Domain, LocalId
from osa.domain.validation.model.hook_release import HookReleaseId
from osa.domain.validation.model.hook_run import HookRun, HookRunId, HookRunStatus


def _dep_srn() -> DepositionSRN:
    return DepositionSRN(domain=Domain("localhost"), id=LocalId("dep-123"))


def test_status_enum_values() -> None:
    assert {s.value for s in HookRunStatus} == {"passed", "warnings", "failed", "error"}


def test_ingest_context_run() -> None:
    run = HookRun(
        id=HookRunId(uuid4()),
        release_id=HookReleaseId(uuid4()),
        ingest_run_id=IngestRunId("ing-1"),
        batch_index=0,
        status=HookRunStatus.PASSED,
        started_at=datetime.now(UTC),
    )
    assert run.ingest_run_id == "ing-1"
    assert run.deposition_id is None
    assert run.oom_retries == 0


def test_deposition_context_run() -> None:
    run = HookRun(
        id=HookRunId(uuid4()),
        release_id=HookReleaseId(uuid4()),
        deposition_id=_dep_srn(),
        status=HookRunStatus.WARNINGS,
        started_at=datetime.now(UTC),
    )
    assert run.deposition_id is not None
    assert run.ingest_run_id is None


def test_exactly_one_execution_context_required() -> None:
    # Both set → invalid.
    with pytest.raises(ValueError):
        HookRun(
            id=HookRunId(uuid4()),
            release_id=HookReleaseId(uuid4()),
            ingest_run_id=IngestRunId("ing-1"),
            deposition_id=_dep_srn(),
            status=HookRunStatus.PASSED,
            started_at=datetime.now(UTC),
        )
    # Neither set → invalid.
    with pytest.raises(ValueError):
        HookRun(
            id=HookRunId(uuid4()),
            release_id=HookReleaseId(uuid4()),
            status=HookRunStatus.PASSED,
            started_at=datetime.now(UTC),
        )
