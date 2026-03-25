"""Record mapper - converts between domain and persistence."""

from datetime import datetime
from typing import Any

from pydantic import TypeAdapter

from osa.domain.record.model.aggregate import Record
from osa.domain.shared.model.source import RecordSource
from osa.domain.shared.model.srn import ConventionSRN, RecordSRN

_source_adapter = TypeAdapter(RecordSource)


def row_to_record(row: dict[str, Any]) -> Record:
    """Convert database row to Record aggregate."""
    published_at = row["published_at"]
    if isinstance(published_at, str):
        published_at = datetime.fromisoformat(published_at)

    source = _source_adapter.validate_python(row["source"])

    return Record(
        srn=RecordSRN.parse(row["srn"]),
        source=source,
        convention_srn=ConventionSRN.parse(row["convention_srn"]),
        metadata=row.get("metadata", {}),
        published_at=published_at,
    )


def record_to_dict(record: Record) -> dict[str, Any]:
    """Convert Record aggregate to database dict."""
    return {
        "srn": str(record.srn),
        "convention_srn": str(record.convention_srn),
        "source": _source_adapter.dump_python(record.source, mode="json"),
        "metadata": record.metadata,
        "published_at": record.published_at,
    }
