"""Unit tests for RecordSource discriminated union."""

import pytest
from pydantic import TypeAdapter, ValidationError

from osa.domain.shared.model.source import (
    DepositionSource,
    HarvestSource,
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


class TestHarvestSource:
    def test_type_is_harvest(self):
        src = HarvestSource(
            id="run-123-source-456",
            harvest_run_srn="urn:osa:localhost:val:run123",
            upstream_source="pdb",
        )
        assert src.type == "harvest"

    def test_requires_harvest_run_srn(self):
        with pytest.raises(ValidationError):
            HarvestSource(id="run-123", upstream_source="pdb")

    def test_requires_upstream_source(self):
        with pytest.raises(ValidationError):
            HarvestSource(id="run-123", harvest_run_srn="urn:osa:localhost:val:run123")

    def test_serialization_roundtrip(self):
        src = HarvestSource(
            id="run-123-source-456",
            harvest_run_srn="urn:osa:localhost:val:run123",
            upstream_source="pdb",
        )
        data = src.model_dump()
        restored = HarvestSource.model_validate(data)
        assert restored == src


class TestRecordSourceDiscriminator:
    def test_deserializes_deposition(self):
        adapter = TypeAdapter(RecordSource)
        src = adapter.validate_python({"type": "deposition", "id": "dep-abc"})
        assert isinstance(src, DepositionSource)

    def test_deserializes_harvest(self):
        data = {
            "type": "harvest",
            "id": "run-123",
            "harvest_run_srn": "urn:osa:localhost:val:run1",
            "upstream_source": "geo",
        }
        adapter = TypeAdapter(RecordSource)
        src = adapter.validate_python(data)
        assert isinstance(src, HarvestSource)
        assert src.upstream_source == "geo"

    def test_rejects_unknown_type(self):
        adapter = TypeAdapter(RecordSource)
        with pytest.raises(ValidationError):
            adapter.validate_python({"type": "unknown", "id": "abc"})

    def test_json_roundtrip(self):
        """Serialize to JSON and back via the union type."""
        adapter = TypeAdapter(RecordSource)
        src = HarvestSource(
            id="run-1",
            harvest_run_srn="urn:osa:localhost:val:run1",
            upstream_source="pdb",
        )
        json_str = adapter.dump_json(src)
        restored = adapter.validate_json(json_str)
        assert isinstance(restored, HarvestSource)
        assert restored == src
