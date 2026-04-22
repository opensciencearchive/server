"""FR-008: Record.schema_id is immutable after construction."""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from osa.domain.record.model.aggregate import Record
from osa.domain.shared.model.source import DepositionSource
from osa.domain.shared.model.srn import ConventionSRN, RecordSRN, SchemaId


def _make_record() -> Record:
    return Record(
        srn=RecordSRN.parse("urn:osa:localhost:rec:abc@1"),
        source=DepositionSource(id="urn:osa:localhost:dep:d1"),
        convention_srn=ConventionSRN.parse("urn:osa:localhost:conv:test@1.0.0"),
        schema_id=SchemaId.parse("test@1.0.0"),
        metadata={"title": "T"},
        published_at=datetime.now(UTC),
    )


def test_schema_id_cannot_be_reassigned():
    record = _make_record()
    other = SchemaId.parse("other@1.0.0")
    with pytest.raises(ValidationError):
        record.schema_id = other  # type: ignore[misc]
