"""Validation API routes."""

from datetime import datetime

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from osa.domain.validation.model import (
    HookResult,
    HookStatus,
    RunStatus,
)
from osa.domain.validation.service.validation import ValidationService


router = APIRouter(
    prefix="/validation",
    tags=["validation"],
    route_class=DishkaRoute,
)


# =============================================================================
# DTOs
# =============================================================================


class HookResultDTO(BaseModel):
    hook_name: str
    status: HookStatus
    rejection_reason: str | None = None
    error_message: str | None = None
    duration_seconds: float


class ValidationStatusResponse(BaseModel):
    """Response with validation run status and results."""

    run_id: str
    status: RunStatus
    summary: HookStatus | None = Field(
        None,
        description="Overall hook result (only set when completed)",
    )
    progress: dict | None = Field(
        None,
        description="Progress info while running",
    )
    results: list[HookResultDTO] = Field(
        default_factory=list,
        description="Individual hook results",
    )
    started_at: datetime | None = None
    completed_at: datetime | None = None


# =============================================================================
# Helpers
# =============================================================================


def _compute_summary(results: list[HookResult]) -> HookStatus | None:
    """Compute overall summary from individual hook results."""
    if not results:
        return None

    statuses = [r.status for r in results]

    if HookStatus.FAILED in statuses:
        return HookStatus.FAILED
    if HookStatus.REJECTED in statuses:
        return HookStatus.REJECTED
    return HookStatus.PASSED


# =============================================================================
# Validation API
# =============================================================================


@router.get(
    "/runs/{run_id}",
    response_model=ValidationStatusResponse,
    description="Get the status and results of a validation run.",
)
async def get_validation_status(
    run_id: str,
    service: FromDishka[ValidationService],
) -> ValidationStatusResponse:
    run = await service.get_run(run_id)
    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Validation run not found: {run_id}",
        )

    results_dto = [
        HookResultDTO(
            hook_name=r.hook_name,
            status=r.status,
            rejection_reason=r.rejection_reason,
            error_message=r.error_message,
            duration_seconds=r.duration_seconds,
        )
        for r in run.results
    ]

    summary = None
    progress = None

    if run.status == RunStatus.COMPLETED or run.status == RunStatus.FAILED:
        summary = _compute_summary(run.results)
    elif run.status == RunStatus.RUNNING:
        progress = {"status": "running"}

    return ValidationStatusResponse(
        run_id=run_id,
        status=run.status,
        summary=summary,
        progress=progress,
        results=results_dto,
        started_at=run.started_at,
        completed_at=run.completed_at,
    )
