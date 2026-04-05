"""Unit tests for RecordSource discriminated union."""

import pytest
from pydantic import TypeAdapter, ValidationError

from osa.domain.shared.model.source import (
    DepositionSource,
    IngestSource,
    RecordSource,
)


class TestDepositionSource:
    def test_type_is_deposition(self):
        src = DepositionSource(id="urn:osa:localhost:dep:abc")
        assert src.type == "deposition"

    def test_id_required(self):
        with pytest.raises(ValidationError):
            DepositionSource(id="")

    def test_serialization_roundtrip(self):
        src = DepositionSource(id="urn:osa:localhost:dep:abc")
        data = src.model_dump()
        assert data == {"type": "deposition", "id": "urn:osa:localhost:dep:abc"}
        restored = DepositionSource.model_validate(data)
        assert restored == src


class TestIngestSource:
    def test_type_is_ingest(self):
        src = IngestSource(
            id="run-123-source-456",
            ingest_run_id="run123",
            upstream_source="pdb",
        )
        assert src.type == "ingest"

    def test_requires_ingest_run_id(self):
        with pytest.raises(ValidationError):
            IngestSource(id="run-123", upstream_source="pdb")

    def test_requires_upstream_source(self):
        with pytest.raises(ValidationError):
            IngestSource(id="run-123", ingest_run_id="run123")

    def test_serialization_roundtrip(self):
        src = IngestSource(
            id="run-123-source-456",
            ingest_run_id="run123",
            upstream_source="pdb",
        )
        data = src.model_dump()
        restored = IngestSource.model_validate(data)
        assert restored == src


class TestRecordSourceDiscriminator:
    def test_deserializes_deposition(self):
        adapter = TypeAdapter(RecordSource)
        src = adapter.validate_python({"type": "deposition", "id": "dep-abc"})
        assert isinstance(src, DepositionSource)

    def test_deserializes_ingest(self):
        data = {
            "type": "ingest",
            "id": "run-123",
            "ingest_run_id": "urn:osa:localhost:val:run1",
            "upstream_source": "geo",
        }
        adapter = TypeAdapter(RecordSource)
        src = adapter.validate_python(data)
        assert isinstance(src, IngestSource)
        assert src.upstream_source == "geo"

    def test_rejects_unknown_type(self):
        adapter = TypeAdapter(RecordSource)
        with pytest.raises(ValidationError):
            adapter.validate_python({"type": "unknown", "id": "abc"})

    def test_json_roundtrip(self):
        """Serialize to JSON and back via the union type."""
        adapter = TypeAdapter(RecordSource)
        src = IngestSource(
            id="run-1",
            ingest_run_id="run1",
            upstream_source="pdb",
        )
        json_str = adapter.dump_json(src)
        restored = adapter.validate_json(json_str)
        assert isinstance(restored, IngestSource)
        assert restored == src
