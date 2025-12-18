from typing import Any, Dict

from osa.domain.deposition.model.aggregate import Deposition
from osa.domain.deposition.model.value import DepositionFile, DepositionStatus
from osa.domain.shared.model.srn import ConventionSRN, DepositionSRN, RecordSRN


def row_to_deposition(row: Dict[str, Any]) -> Deposition[Dict[str, Any]]:
    """Convert database row to Deposition aggregate.

    Note: We assume the payload is a dict. Since Deposition is generic,
    at the persistence boundary we treat it as a dict.
    """
    files_data = row.get("files", []) or []
    files = [DepositionFile(**f) for f in files_data]

    record_id = row.get("record_id")

    return Deposition(
        srn=DepositionSRN.parse(row["srn"]),
        convention_srn=ConventionSRN.parse(row["profile_srn"]),
        status=DepositionStatus(row["status"]),
        payload=row.get("payload", {}),
        files=files,
        record_srn=RecordSRN.parse(record_id) if record_id else None,
    )


def deposition_to_dict(dep: Deposition) -> Dict[str, Any]:
    """Convert Deposition aggregate to database dict."""
    return {
        "srn": str(dep.srn),
        "profile_srn": str(dep.convention_srn),
        "status": dep.status.value,
        "payload": dep.payload
        if isinstance(dep.payload, dict)
        else dep.payload.model_dump(),
        "files": [f.model_dump(mode="json") for f in dep.files],
        "record_id": str(dep.record_srn) if dep.record_srn else None,
    }
