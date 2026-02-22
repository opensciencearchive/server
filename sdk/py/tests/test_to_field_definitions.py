"""Tests for MetadataSchema.to_field_definitions() — maps Python types to server FieldDefinitions."""

from __future__ import annotations

from datetime import date, datetime

from osa import Field, MetadataSchema


class SimpleSchema(MetadataSchema):
    title: str
    count: int
    score: float
    active: bool


class DateSchema(MetadataSchema):
    created: date
    updated: datetime


class OptionalSchema(MetadataSchema):
    name: str
    description: str | None = None
    score: float | None = Field(default=None, unit="Å")


class UnitSchema(MetadataSchema):
    resolution: float | None = Field(default=None, unit="Å")
    weight: float = Field(unit="kDa")


class TestToFieldDefinitions:
    def test_str_maps_to_text(self) -> None:
        fields = SimpleSchema.to_field_definitions()
        title_field = next(f for f in fields if f["name"] == "title")
        assert title_field["type"] == "text"
        assert title_field["required"] is True

    def test_int_maps_to_number_integer_only(self) -> None:
        fields = SimpleSchema.to_field_definitions()
        count_field = next(f for f in fields if f["name"] == "count")
        assert count_field["type"] == "number"
        assert count_field["constraints"]["type"] == "number"
        assert count_field["constraints"]["integer_only"] is True

    def test_float_maps_to_number(self) -> None:
        fields = SimpleSchema.to_field_definitions()
        score_field = next(f for f in fields if f["name"] == "score")
        assert score_field["type"] == "number"
        # float has number constraints but not integer_only
        assert score_field["constraints"]["type"] == "number"
        assert score_field["constraints"].get("integer_only") is not True

    def test_bool_maps_to_boolean(self) -> None:
        fields = SimpleSchema.to_field_definitions()
        active_field = next(f for f in fields if f["name"] == "active")
        assert active_field["type"] == "boolean"
        assert active_field["required"] is True

    def test_date_maps_to_date(self) -> None:
        fields = DateSchema.to_field_definitions()
        created_field = next(f for f in fields if f["name"] == "created")
        assert created_field["type"] == "date"

    def test_datetime_maps_to_date(self) -> None:
        fields = DateSchema.to_field_definitions()
        updated_field = next(f for f in fields if f["name"] == "updated")
        assert updated_field["type"] == "date"

    def test_optional_field_not_required(self) -> None:
        fields = OptionalSchema.to_field_definitions()
        desc_field = next(f for f in fields if f["name"] == "description")
        assert desc_field["required"] is False

    def test_required_field_is_required(self) -> None:
        fields = OptionalSchema.to_field_definitions()
        name_field = next(f for f in fields if f["name"] == "name")
        assert name_field["required"] is True

    def test_unit_in_constraints(self) -> None:
        fields = UnitSchema.to_field_definitions()
        res_field = next(f for f in fields if f["name"] == "resolution")
        assert res_field["constraints"]["unit"] == "Å"

    def test_unit_on_required_field(self) -> None:
        fields = UnitSchema.to_field_definitions()
        weight_field = next(f for f in fields if f["name"] == "weight")
        assert weight_field["constraints"]["unit"] == "kDa"
        assert weight_field["required"] is True

    def test_cardinality_defaults_to_exactly_one(self) -> None:
        fields = SimpleSchema.to_field_definitions()
        for f in fields:
            assert f["cardinality"] == "exactly_one"

    def test_returns_list_of_dicts(self) -> None:
        fields = SimpleSchema.to_field_definitions()
        assert isinstance(fields, list)
        assert all(isinstance(f, dict) for f in fields)
        assert len(fields) == 4
