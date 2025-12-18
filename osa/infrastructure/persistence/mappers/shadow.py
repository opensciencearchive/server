from typing import Any, Dict

from osa.domain.shadow.model.aggregate import ShadowId, ShadowRequest
from osa.domain.shadow.model.report import ShadowReport
from osa.domain.shadow.model.value import ShadowStatus
from osa.domain.shared.model.srn import ConventionSRN, DepositionSRN


def row_to_shadow_request(row: Dict[str, Any]) -> ShadowRequest:
    """Convert database row to ShadowRequest domain model."""
    dep_id_str = row.get("deposition_id")

    return ShadowRequest(
        id=ShadowId(row["id"]),
        status=ShadowStatus(row["status"]),
        source_url=row["source_url"],
        convention_srn=ConventionSRN.parse(row["profile_srn"]),
        deposition_id=DepositionSRN.parse(dep_id_str) if dep_id_str else None,
    )


def shadow_request_to_dict(req: ShadowRequest) -> Dict[str, Any]:
    """Convert ShadowRequest domain model to database dict."""
    return {
        "id": req.id,
        "status": req.status.value,
        "source_url": req.source_url,
        "profile_srn": str(req.convention_srn),
        "deposition_id": str(req.deposition_id) if req.deposition_id else None,
    }


def row_to_shadow_report(row: Dict[str, Any]) -> ShadowReport:
    """Convert database row to ShadowReport domain model."""
    return ShadowReport(
        shadow_id=ShadowId(row["shadow_id"]),
        source_domain=row["source_domain"],
        validation_summary=row["validation_summary"],
        score=row["score"],
        created_at=row["created_at"],
    )


def shadow_report_to_dict(report: ShadowReport) -> Dict[str, Any]:
    """Convert ShadowReport domain model to database dict."""
    return report.model_dump()
