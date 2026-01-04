from typing import Any

from osa.domain.deposition.model.aggregate import Deposition
from osa.domain.deposition.model.value import DepositionFile, DepositionStatus
from osa.domain.shared.model.srn import DepositionSRN, RecordSRN


def row_to_deposition(row: dict[str, Any]) -> Deposition[dict[str, Any]]:
    """Convert database row to Deposition aggregate.

    Note: We assume the metadata is a dict. Since Deposition is generic,
    at the persistence boundary we treat it as a dict.
    """
    files_data = row.get("files", []) or []
    files = [DepositionFile(**f) for f in files_data]

    record_id = row.get("record_id")

    return Deposition(
        srn=DepositionSRN.parse(row["srn"]),
        status=DepositionStatus(row["status"]),
        metadata=row.get("metadata", {}),
        files=files,
        provenance=row.get("provenance", {}),
        record_srn=RecordSRN.parse(record_id) if record_id else None,
    )


def deposition_to_dict(dep: Deposition) -> dict[str, Any]:
    """Convert Deposition aggregate to database dict."""
    return {
        "srn": str(dep.srn),
        "status": dep.status,
        "metadata": dep.metadata if isinstance(dep.metadata, dict) else dep.metadata.model_dump(),
        "files": [f.model_dump(mode="json") for f in dep.files],
        "provenance": dep.provenance,
        "record_id": str(dep.record_srn) if dep.record_srn else None,
    }
