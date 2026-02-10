from typing import Any
from uuid import UUID

from osa.domain.auth.model.value import UserId
from osa.domain.deposition.model.aggregate import Deposition
from osa.domain.deposition.model.value import DepositionFile, DepositionStatus
from osa.domain.shared.model.srn import ConventionSRN, DepositionSRN, RecordSRN


def row_to_deposition(row: dict[str, Any]) -> Deposition:
    """Convert database row to Deposition aggregate."""
    files_data = row.get("files", []) or []
    files = [DepositionFile(**f) for f in files_data]

    record_id = row.get("record_id")

    return Deposition(
        srn=DepositionSRN.parse(row["srn"]),
        convention_srn=ConventionSRN.parse(row["convention_srn"]),
        status=DepositionStatus(row["status"]),
        metadata=row.get("metadata", {}),
        files=files,
        record_srn=RecordSRN.parse(record_id) if record_id else None,
        owner_id=UserId(UUID(row["owner_id"])),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def deposition_to_dict(dep: Deposition) -> dict[str, Any]:
    """Convert Deposition aggregate to database dict."""
    return {
        "srn": str(dep.srn),
        "convention_srn": str(dep.convention_srn),
        "status": dep.status,
        "metadata": dep.metadata,
        "files": [f.model_dump(mode="json") for f in dep.files],
        "record_id": str(dep.record_srn) if dep.record_srn else None,
        "owner_id": str(dep.owner_id),
        "created_at": dep.created_at,
        "updated_at": dep.updated_at,
    }
