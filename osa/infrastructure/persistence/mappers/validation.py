from typing import Any

from osa.domain.shared.model.srn import TraitSRN, ValidationRunSRN
from osa.domain.validation.model import (
    CheckResult,
    Trait,
    TraitStatus,
    ValidationRun,
    Validator,
    ValidatorLimits,
    ValidatorRef,
)
from osa.domain.validation.model.value import CheckStatus, RunStatus


def row_to_trait(row: dict[str, Any]) -> Trait:
    """Convert database row to Trait aggregate."""
    validator_data = row["validator"]
    return Trait(
        srn=TraitSRN.parse(row["srn"]),
        slug=row["slug"],
        name=row["name"],
        description=row["description"],
        validator=Validator(
            ref=ValidatorRef(**validator_data["ref"]),
            limits=ValidatorLimits(**validator_data.get("limits", {})),
        ),
        status=TraitStatus(row["status"]),
        created_at=row["created_at"],
    )


def trait_to_dict(trait: Trait) -> dict[str, Any]:
    """Convert Trait aggregate to database dict."""
    return {
        "srn": str(trait.srn),
        "slug": trait.slug,
        "name": trait.name,
        "description": trait.description,
        "validator": trait.validator.model_dump(mode="json"),
        "status": trait.status.value,
        "created_at": trait.created_at,
    }


def row_to_validation_run(row: dict[str, Any]) -> ValidationRun:
    """Convert database row to ValidationRun entity."""
    trait_srns_data = row.get("trait_srns", []) or []
    trait_srns = [TraitSRN.parse(s) for s in trait_srns_data]

    results_data = row.get("results", []) or []
    results = [
        CheckResult(
            trait_srn=r["trait_srn"],
            validator_digest=r["validator_digest"],
            status=CheckStatus(r["status"]),
            message=r.get("message"),
            details=r.get("details"),
        )
        for r in results_data
    ]
    return ValidationRun(
        srn=ValidationRunSRN.parse(row["srn"]),
        trait_srns=trait_srns,
        status=RunStatus(row["status"]),
        results=results,
        started_at=row.get("started_at"),
        completed_at=row.get("completed_at"),
        expires_at=row.get("expires_at"),
    )


def validation_run_to_dict(run: ValidationRun) -> dict[str, Any]:
    """Convert ValidationRun entity to database dict."""
    return {
        "srn": str(run.srn),
        "trait_srns": [str(t) for t in run.trait_srns],
        "status": run.status.value,
        "results": [r.model_dump(mode="json") for r in run.results],
        "started_at": run.started_at,
        "completed_at": run.completed_at,
        "expires_at": run.expires_at,
    }
