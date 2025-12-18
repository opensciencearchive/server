import asyncio
import uuid
from datetime import datetime, timedelta, timezone

from dishka.integrations.fastapi import FromDishka
from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel, Field

from osa.config import Config
from osa.domain.shared.model.srn import (
    Domain,
    LocalId,
    Semver,
    TraitSRN,
    ValidationRunSRN,
)
from osa.domain.validation.command import RegisterTrait, RegisterTraitHandler
from osa.domain.validation.model import (
    CheckResult,
    CheckStatus,
    RunStatus,
    TraitStatus,
    ValidationRun,
    Validator,
    ValidatorLimits,
    ValidatorRef,
)
from osa.domain.validation.port.repository import TraitRepository
from osa.domain.validation.port.runner import ValidationInputs
from osa.domain.validation.service import ValidationService


router = APIRouter(tags=["validation"])


# =============================================================================
# DTOs
# =============================================================================


class ValidatorRefDTO(BaseModel):
    image: str
    digest: str


class ValidatorLimitsDTO(BaseModel):
    timeout_seconds: int = 60
    memory: str = "256Mi"
    cpu: str = "0.5"


class ValidatorDTO(BaseModel):
    ref: ValidatorRefDTO
    limits: ValidatorLimitsDTO = ValidatorLimitsDTO()


class TraitDTO(BaseModel):
    srn: str
    slug: str
    name: str
    description: str
    validator: ValidatorDTO
    status: TraitStatus
    created_at: datetime


class CreateTraitDTO(BaseModel):
    slug: str
    name: str
    description: str
    validator: ValidatorDTO
    version: str = "1.0.0"


class CheckResultDTO(BaseModel):
    trait_srn: str
    validator_digest: str
    status: CheckStatus
    message: str | None = None
    details: dict | None = None


class TraitSRNInput(BaseModel):
    """Reference to a trait for validation."""

    domain: str = Field(..., description="Domain of the trait (e.g., 'osap.org')")
    id: str = Field(..., description="Trait identifier (e.g., 'si-units')")
    version: str = Field(..., description="Trait version (e.g., '1.0.0')")


class ValidateRequest(BaseModel):
    """Request to validate data against a set of traits."""

    trait_srns: list[TraitSRNInput] = Field(
        ...,
        description="List of traits to validate against",
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
        description="Individual trait validation results",
    )
    started_at: datetime | None = None
    completed_at: datetime | None = None


# =============================================================================
# Helpers
# =============================================================================


def _trait_to_dto(trait) -> TraitDTO:
    return TraitDTO(
        srn=str(trait.srn),
        slug=trait.slug,
        name=trait.name,
        description=trait.description,
        validator=ValidatorDTO(
            ref=ValidatorRefDTO(
                image=trait.validator.ref.image,
                digest=trait.validator.ref.digest,
            ),
            limits=ValidatorLimitsDTO(
                timeout_seconds=trait.validator.limits.timeout_seconds,
                memory=trait.validator.limits.memory,
                cpu=trait.validator.limits.cpu,
            ),
        ),
        status=trait.status,
        created_at=trait.created_at,
    )


def _compute_summary(results: list) -> CheckStatus | None:
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
# Traits Routes
# =============================================================================


@router.get(
    "/traits",
    response_model=list[TraitDTO],
    description="List all available traits",
)
async def list_traits(
    trait_repo: FromDishka[TraitRepository],
) -> list[TraitDTO]:
    traits = await trait_repo.list()
    return [_trait_to_dto(t) for t in traits]


@router.post(
    "/traits",
    status_code=status.HTTP_201_CREATED,
    response_model=TraitDTO,
    description="Register a new trait",
)
async def register_trait(
    payload: CreateTraitDTO,
    handler: FromDishka[RegisterTraitHandler],
    trait_repo: FromDishka[TraitRepository],
    config: FromDishka[Config],
) -> TraitDTO:
    # Build SRN from node domain + slug + version
    trait_srn = TraitSRN(
        domain=Domain(config.server.domain),
        id=LocalId(payload.slug),
        version=Semver(payload.version),
    )

    # Check if trait already exists
    existing = await trait_repo.get(trait_srn)
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Trait already exists: {trait_srn}",
        )

    # Build domain objects
    validator = Validator(
        ref=ValidatorRef(
            image=payload.validator.ref.image,
            digest=payload.validator.ref.digest,
        ),
        limits=ValidatorLimits(
            timeout_seconds=payload.validator.limits.timeout_seconds,
            memory=payload.validator.limits.memory,
            cpu=payload.validator.limits.cpu,
        ),
    )

    cmd = RegisterTrait(
        srn=trait_srn,
        slug=payload.slug,
        name=payload.name,
        description=payload.description,
        validator=validator,
    )
    await handler.run(cmd)

    # Fetch the created trait to return
    trait = await trait_repo.get(trait_srn)
    if not trait:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Trait was not created",
        )

    return _trait_to_dto(trait)


@router.get(
    "/traits/{trait_id}",
    response_model=TraitDTO,
    description="Get a trait by ID and version",
)
async def get_trait(
    trait_id: str,
    trait_repo: FromDishka[TraitRepository],
    config: FromDishka[Config],
    version: str = Query(..., description="Trait version (semver)"),
) -> TraitDTO:
    trait_srn = TraitSRN(
        domain=Domain(config.server.domain),
        id=LocalId(trait_id),
        version=Semver(version),
    )
    trait = await trait_repo.get(trait_srn)
    if not trait:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Trait not found: {trait_id}@{version}",
        )
    return _trait_to_dto(trait)


# =============================================================================
# Validation API
# =============================================================================


@router.post(
    "/validate",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=ValidateResponse,
    description="Submit data for validation against traits. Returns immediately with a run ID for polling.",
)
async def submit_validation(
    payload: ValidateRequest,
    service: FromDishka[ValidationService],
) -> ValidateResponse:
    # Convert DTOs to domain objects
    trait_srns = [
        TraitSRN(
            domain=Domain(t.domain),
            id=LocalId(t.id),
            version=Semver(t.version),
        )
        for t in payload.trait_srns
    ]

    inputs = ValidationInputs(record_json=payload.record)
    expires_at = datetime.now(timezone.utc) + timedelta(hours=24)

    # Create the run (pending) and spawn background validation
    run_srn = ValidationRunSRN(
        domain=service.node_domain,
        id=LocalId(str(uuid.uuid4())),
        version=None,
    )
    run = ValidationRun(
        srn=run_srn,
        trait_srns=trait_srns,
        status=RunStatus.PENDING,
        expires_at=expires_at,
    )
    await service.run_repo.save(run)

    # Spawn background task
    asyncio.create_task(
        _run_validation_background(service, run, trait_srns, inputs),
        name=f"validation-{run_srn.id}",
    )

    run_id = str(run.srn.id)
    return ValidateResponse(
        run_id=run_id,
        status=run.status,
        poll_url=f"/validate/{run_id}",
    )


async def _run_validation_background(
    service: ValidationService,
    run: ValidationRun,
    trait_srns: list[TraitSRN],
    inputs: ValidationInputs,
) -> None:
    """Run validation in background, updating the run status."""
    try:
        run.status = RunStatus.RUNNING
        run.started_at = datetime.now(timezone.utc)
        await service.run_repo.save(run)

        results: list[CheckResult] = []
        overall_failed = False

        for trait_srn in trait_srns:
            result = await service._validate_trait(trait_srn, inputs)
            results.append(result)
            if result.status in (CheckStatus.FAILED, CheckStatus.ERROR):
                overall_failed = True

        run.results = results
        run.status = RunStatus.FAILED if overall_failed else RunStatus.COMPLETED
        run.completed_at = datetime.now(timezone.utc)
        await service.run_repo.save(run)

    except Exception as e:
        run.status = RunStatus.FAILED
        run.completed_at = datetime.now(timezone.utc)
        run.results = [
            CheckResult(
                trait_srn="system",
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
            trait_srn=r.trait_srn,
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
