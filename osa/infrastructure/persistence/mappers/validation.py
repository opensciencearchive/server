from typing import Any

from osa.domain.shared.model.srn import ValidationRunSRN
from osa.domain.validation.model import CheckResult, ValidationRun
from osa.domain.validation.model.value import CheckStatus, RunStatus


def row_to_validation_run(row: dict[str, Any]) -> ValidationRun:
    """Convert database row to ValidationRun entity."""
    results_data = row.get("results", []) or []
    results = [
        CheckResult(
            check_id=r["check_id"],
            validator_digest=r["validator_digest"],
            status=CheckStatus(r["status"]),
            message=r.get("message"),
            details=r.get("details"),
        )
        for r in results_data
    ]
    return ValidationRun(
        srn=ValidationRunSRN.parse(row["srn"]),
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
        "status": run.status.value,
        "results": [r.model_dump(mode="json") for r in run.results],
        "started_at": run.started_at,
        "completed_at": run.completed_at,
        "expires_at": run.expires_at,
    }
