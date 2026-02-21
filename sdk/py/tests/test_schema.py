"""Tests for MetadataSchema and Field()."""

from typing import Literal

import pytest
from pydantic import ValidationError

from osa import Field, MetadataSchema


class ProteinStructure(MetadataSchema):
    """Schema for protein structure deposits."""

    organism: str
    method: Literal["xray", "cryo-em", "nmr", "predicted"]
    resolution: float | None = Field(default=None, ge=0, le=100, unit="Å")
    uniprot_id: str = Field(pattern=r"^[A-Z0-9]{6,10}$")


VALID_DATA = {
    "organism": "H. sapiens",
    "method": "xray",
    "resolution": 2.1,
    "uniprot_id": "P12345",
}


class TestMetadataSchema:
    def test_valid_data_creates_typed_instance(self) -> None:
        ps = ProteinStructure(**VALID_DATA)
        assert ps.organism == "H. sapiens"
        assert ps.method == "xray"
        assert ps.resolution == 2.1
        assert ps.uniprot_id == "P12345"

    def test_missing_required_field_raises(self) -> None:
        with pytest.raises(ValidationError):
            ProteinStructure(organism="H. sapiens", method="xray")

    def test_value_outside_range_raises(self) -> None:
        with pytest.raises(ValidationError):
            ProteinStructure(**{**VALID_DATA, "resolution": -1.0})
        with pytest.raises(ValidationError):
            ProteinStructure(**{**VALID_DATA, "resolution": 101.0})

    def test_pattern_mismatch_raises(self) -> None:
        with pytest.raises(ValidationError):
            ProteinStructure(**{**VALID_DATA, "uniprot_id": "invalid!"})

    def test_default_value_applied(self) -> None:
        ps = ProteinStructure(organism="H. sapiens", method="xray", uniprot_id="P12345")
        assert ps.resolution is None

    def test_extra_field_raises(self) -> None:
        with pytest.raises(ValidationError):
            ProteinStructure(**{**VALID_DATA, "extra_field": "nope"})

    def test_json_schema_has_correct_types(self) -> None:
        schema = ProteinStructure.model_json_schema()
        props = schema["properties"]
        assert props["organism"]["type"] == "string"
        assert props["resolution"]["anyOf"] == [
            {"type": "number", "maximum": 100.0, "minimum": 0.0},
            {"type": "null"},
        ]
        assert "enum" in props["method"]

    def test_unit_metadata_in_json_schema(self) -> None:
        schema = ProteinStructure.model_json_schema()
        res_schema = schema["properties"]["resolution"]
        assert res_schema.get("unit") == "Å"
