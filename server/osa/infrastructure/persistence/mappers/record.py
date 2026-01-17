"""Record mapper - converts between domain and persistence."""

from datetime import datetime
from typing import Any

from osa.domain.record.model.aggregate import Record
from osa.domain.record.model.value import IndexRef
from osa.domain.shared.model.srn import DepositionSRN, RecordSRN


def row_to_record(row: dict[str, Any]) -> Record:
    """Convert database row to Record aggregate."""
    published_at = row["published_at"]
    if isinstance(published_at, str):
        published_at = datetime.fromisoformat(published_at)

    # Deserialize indexes
    raw_indexes = row.get("indexes", {}) or {}
    indexes: dict[str, IndexRef] = {
        key: IndexRef.model_validate(value) for key, value in raw_indexes.items()
    }

    return Record(
        srn=RecordSRN.parse(row["srn"]),
        deposition_srn=DepositionSRN.parse(row["deposition_srn"]),
        metadata=row.get("metadata", {}),
        indexes=indexes,
        published_at=published_at,
    )


def record_to_dict(record: Record) -> dict[str, Any]:
    """Convert Record aggregate to database dict."""
    indexes_dict = {key: ref.model_dump(mode="json") for key, ref in record.indexes.items()}

    return {
        "srn": str(record.srn),
        "deposition_srn": str(record.deposition_srn),
        "metadata": record.metadata,
        "indexes": indexes_dict,
        "published_at": record.published_at,
    }
