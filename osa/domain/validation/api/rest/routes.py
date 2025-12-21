import asyncio
import uuid
from datetime import datetime, timedelta, timezone

from dishka.integrations.fastapi import FromDishka
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from osa.domain.shared.model.srn import LocalId, ValidationRunSRN
from osa.domain.validation.model import (
    CheckResult,
    CheckStatus,
    RunStatus,
    ValidationRun,
)
from osa.domain.validation.port.runner import ValidationInputs
from osa.domain.validation.service import ValidationService


router = APIRouter(tags=["validation"])


# =============================================================================
# DTOs
# =============================================================================


class ValidatorInput(BaseModel):
    """A validator to run."""

    image: str = Field(..., description="OCI image reference")
    digest: str = Field(..., description="Image digest (sha256:...)")


class CheckResultDTO(BaseModel):
    check_id: str
    validator_digest: str
    status: CheckStatus
    message: str | None = None
    details: dict | None = None


class ValidateRequest(BaseModel):
    """Request to validate data against validators."""

    validators: list[ValidatorInput] = Field(
        ...,
        description="List of validators to run",
        min_length=1,
    )
    record: dict = Field(
        ...,
        description="The data record to validate (JSON object)",
    )


class ValidateResponse(BaseModel):
    """Response from submitting a validation request."""

    run_id: str = Field(..., description="Unique ID for this validation run")
    status: RunStatus = Field(..., description="Current status of the validation")
    poll_url: str = Field(..., description="URL to poll for results")


class ValidationStatusResponse(BaseModel):
    """Response with validation run status and results."""

    run_id: str
    status: RunStatus
    summary: CheckStatus | None = Field(
        None,
        description="Overall validation result (only set when completed)",
    )
    progress: dict | None = Field(
        None,
        description="Progress info (completed/total) while running",
    )
    results: list[CheckResultDTO] = Field(
        default_factory=list,
        description="Individual validation results",
    )
    started_at: datetime | None = None
    completed_at: datetime | None = None


# =============================================================================
# Helpers
# =============================================================================


def _compute_summary(results: list[CheckResult]) -> CheckStatus | None:
    """Compute overall summary from individual results."""
    if not results:
        return None

    statuses = [r.status for r in results]

    if CheckStatus.ERROR in statuses:
        return CheckStatus.ERROR
    if CheckStatus.FAILED in statuses:
        return CheckStatus.FAILED
    if CheckStatus.WARNINGS in statuses:
        return CheckStatus.WARNINGS
    return CheckStatus.PASSED


# =============================================================================
# Validation API
# =============================================================================


@router.post(
    "/validate",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=ValidateResponse,
    description="Submit data for validation. Returns immediately with a run ID for polling.",
)
async def submit_validation(
    payload: ValidateRequest,
    service: FromDishka[ValidationService],
) -> ValidateResponse:
    # Create validation run
    inputs = ValidationInputs(record_json=payload.record)
    expires_at = datetime.now(timezone.utc) + timedelta(hours=24)

    run_srn = ValidationRunSRN(
        domain=service.node_domain,
        id=LocalId(str(uuid.uuid4())),
        version=None,
    )
    run = ValidationRun(
        srn=run_srn,
        status=RunStatus.PENDING,
        expires_at=expires_at,
    )
    await service.run_repo.save(run)

    # Spawn background task
    validators = [(v.image, v.digest) for v in payload.validators]
    asyncio.create_task(
        _run_validation_background(service, run, inputs, validators),
        name=f"validation-{run_srn.id}",
    )

    run_id = str(run.srn.id.root)
    return ValidateResponse(
        run_id=run_id,
        status=run.status,
        poll_url=f"/validate/{run_id}",
    )


async def _run_validation_background(
    service: ValidationService,
    run: ValidationRun,
    inputs: ValidationInputs,
    validators: list[tuple[str, str]],
) -> None:
    """Run validation in background, updating the run status."""
    try:
        await service.run_validation(run, inputs, validators)
    except Exception as e:
        run.status = RunStatus.FAILED
        run.completed_at = datetime.now(timezone.utc)
        run.results = [
            CheckResult(
                check_id="system",
                validator_digest="",
                status=CheckStatus.ERROR,
                message=f"Validation failed: {e}",
                details=None,
            )
        ]
        await service.run_repo.save(run)


@router.get(
    "/validate/{run_id}",
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

    # Build response based on status
    results_dto = [
        CheckResultDTO(
            check_id=r.check_id,
            validator_digest=r.validator_digest,
            status=r.status,
            message=r.message,
            details=r.details,
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
