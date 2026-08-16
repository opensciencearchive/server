"""Validation API routes.

Thin HTTP ↔ DTO coercion only: the status→shape rule and the explicit
``public()`` gate live in ``GetValidationRunHandler``; a missing run raises
``NotFoundError``, mapped centrally (arch-survey 2026-08-16 F1 — this route
previously injected ValidationService directly and owned the decision tree).
"""

from datetime import datetime

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter
from pydantic import BaseModel, Field

from osa.domain.validation.model import HookStatus, RunStatus
from osa.domain.validation.query.get_validation_run import (
    GetValidationRun,
    GetValidationRunHandler,
)

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
# Validation API
# =============================================================================


@router.get(
    "/runs/{run_id}",
    response_model=ValidationStatusResponse,
    description="Get the status and results of a validation run.",
)
async def get_validation_status(
    run_id: str,
    handler: FromDishka[GetValidationRunHandler],
) -> ValidationStatusResponse:
    result = await handler.run(GetValidationRun(run_id=run_id))
    return ValidationStatusResponse(
        run_id=result.run_id,
        status=result.status,
        summary=result.summary,
        progress=result.progress.model_dump() if result.progress else None,
        results=[HookResultDTO(**r.model_dump()) for r in result.results],
        started_at=result.started_at,
        completed_at=result.completed_at,
    )
