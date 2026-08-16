"""GetValidationRun — status + results of one validation run.

Owns the rule the route used to hard-code (arch-survey 2026-08-16 F1): which
fields are meaningful at which run status. Terminal runs carry the summary;
running runs carry progress; pending runs carry neither. A missing run raises
``NotFoundError`` for the central error mapper — no route-level HTTPException.
"""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

from osa.domain.shared.authorization.gate import public
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.query import Query, QueryHandler, Result
from osa.domain.validation.model.hook_result import HookStatus
from osa.domain.validation.model.value import RunStatus
from osa.domain.validation.service.validation import ValidationService

_TERMINAL = (RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.REJECTED)


class GetValidationRun(Query):
    run_id: str


class HookResultEntry(Result):
    hook_name: str
    status: HookStatus
    rejection_reason: str | None = None
    error_message: str | None = None
    duration_seconds: float


class RunProgress(BaseModel):
    """Typed progress marker — present only while the run is RUNNING."""

    status: Literal["running"] = "running"


class ValidationRunStatus(Result):
    run_id: str
    status: RunStatus
    summary: HookStatus | None
    progress: RunProgress | None
    results: list[HookResultEntry] = Field(default_factory=list)
    started_at: datetime | None
    completed_at: datetime | None


class GetValidationRunHandler(QueryHandler[GetValidationRun, ValidationRunStatus]):
    """``public()`` is deliberate: depositors poll their run status anonymously
    today (the route had no gate at all). An ownership check is future work —
    and now a one-line gate change away, boot-validated."""

    __auth__ = public()

    service: ValidationService

    async def run(self, cmd: GetValidationRun) -> ValidationRunStatus:
        run = await self.service.get_run(cmd.run_id)
        if run is None:
            raise NotFoundError(f"Validation run not found: {cmd.run_id}")
        return ValidationRunStatus(
            run_id=cmd.run_id,
            status=run.status,
            summary=run.summary if run.status in _TERMINAL else None,
            progress=RunProgress() if run.status == RunStatus.RUNNING else None,
            results=[
                HookResultEntry(
                    hook_name=r.hook_name.root,
                    status=r.status,
                    rejection_reason=r.rejection_reason,
                    error_message=r.error_message,
                    duration_seconds=r.duration_seconds,
                )
                for r in run.results
            ],
            started_at=run.started_at,
            completed_at=run.completed_at,
        )
