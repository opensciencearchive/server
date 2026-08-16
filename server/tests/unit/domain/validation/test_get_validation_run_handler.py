"""GetValidationRun query handler (arch-survey 2026-08-16 F1).

The /validation/runs/{id} route previously injected ValidationService directly
— no __auth__ gate existed on the path, and the route owned the only
business-logic decision tree in a route file (which fields are meaningful at
which run status). Both now live here: the handler carries an explicit gate
and owns the status→shape rule; the route is thin.
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from osa.domain.shared.authorization.gate import Public
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.model.srn import Domain, LocalId, ValidationRunSRN
from osa.domain.validation.model.entity import ValidationRun
from osa.domain.validation.model.hook_result import HookResult, HookStatus
from osa.domain.validation.model.value import RunStatus
from osa.domain.validation.query.get_validation_run import (
    GetValidationRun,
    GetValidationRunHandler,
)


def _run(status: RunStatus, results: list[HookResult] | None = None) -> ValidationRun:
    return ValidationRun(
        srn=ValidationRunSRN(domain=Domain("localhost"), id=LocalId("run-000000001"), version=None),
        status=status,
        results=results or [],
        started_at=datetime(2026, 1, 1, tzinfo=UTC),
        completed_at=None,
    )


def _passed_result() -> HookResult:
    from osa.domain.shared.model.hook import HookName

    return HookResult(hook_name=HookName("quality"), status=HookStatus.PASSED, duration_seconds=1.5)


def _handler(run: ValidationRun | None) -> GetValidationRunHandler:
    service = AsyncMock()
    service.get_run.return_value = run
    return GetValidationRunHandler(service=service)


class TestGate:
    def test_gate_is_explicitly_public(self):
        """The changed contract of F1: an explicit, boot-validated gate exists.
        Public is the deliberate, documented choice (depositors poll their run
        status anonymously today) — tightening it later is a one-line edit."""
        assert isinstance(GetValidationRunHandler.__auth__, Public)


@pytest.mark.asyncio
class TestStatusShaping:
    async def test_terminal_statuses_carry_summary_and_no_progress(self):
        for status in (RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.REJECTED):
            result = await _handler(_run(status, [_passed_result()])).run(
                GetValidationRun(run_id="run-000000001")
            )
            assert result.summary == HookStatus.PASSED, status
            assert result.progress is None, status

    async def test_running_carries_progress_and_no_summary(self):
        result = await _handler(_run(RunStatus.RUNNING)).run(
            GetValidationRun(run_id="run-000000001")
        )
        assert result.summary is None
        assert result.progress is not None and result.progress.status == "running"

    async def test_pending_carries_neither(self):
        result = await _handler(_run(RunStatus.PENDING)).run(
            GetValidationRun(run_id="run-000000001")
        )
        assert result.summary is None and result.progress is None

    async def test_results_are_mapped(self):
        result = await _handler(_run(RunStatus.COMPLETED, [_passed_result()])).run(
            GetValidationRun(run_id="run-000000001")
        )
        assert len(result.results) == 1
        entry = result.results[0]
        assert entry.hook_name == "quality"
        assert entry.status == HookStatus.PASSED
        assert entry.duration_seconds == 1.5

    async def test_missing_run_raises_not_found(self):
        """Central error mapping, not a route-level HTTPException."""
        with pytest.raises(NotFoundError):
            await _handler(None).run(GetValidationRun(run_id="nope-00000001"))
