"""Tests for new FeatureSchema generation from Pydantic BaseModel."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel

from osa.types.record import Record
from osa.types.schema import MetadataSchema


class SampleSchema(MetadataSchema):
    organism: str


class PocketResult(BaseModel):
    pocket_id: str
    score: float
    volume: float
    n_spheres: int


class SimpleResult(BaseModel):
    name: str
    ok: bool
    count: int


class AllTypesResult(BaseModel):
    text_field: str
    number_field: float
    integer_field: int
    boolean_field: bool
    optional_field: str | None = None
    list_field: list[int] = []
    dict_field: dict[str, Any] = {}


class DateResult(BaseModel):
    created: datetime
    id: UUID


class TestFeatureSchemaGeneration:
    def setup_method(self) -> None:
        from osa._registry import clear

        clear()

    def test_generates_column_defs_from_model(self) -> None:
        from osa.manifest import generate_feature_schema

        schema = generate_feature_schema(PocketResult)
        assert len(schema.columns) == 4
        names = [c.name for c in schema.columns]
        assert "pocket_id" in names
        assert "score" in names
        assert "volume" in names
        assert "n_spheres" in names

    def test_maps_str_to_string(self) -> None:
        from osa.manifest import generate_feature_schema

        schema = generate_feature_schema(SimpleResult)
        name_col = next(c for c in schema.columns if c.name == "name")
        assert name_col.json_type == "string"

    def test_maps_float_to_number(self) -> None:
        from osa.manifest import generate_feature_schema

        schema = generate_feature_schema(PocketResult)
        score_col = next(c for c in schema.columns if c.name == "score")
        assert score_col.json_type == "number"

    def test_maps_int_to_integer(self) -> None:
        from osa.manifest import generate_feature_schema

        schema = generate_feature_schema(PocketResult)
        n_col = next(c for c in schema.columns if c.name == "n_spheres")
        assert n_col.json_type == "integer"

    def test_maps_bool_to_boolean(self) -> None:
        from osa.manifest import generate_feature_schema

        schema = generate_feature_schema(SimpleResult)
        ok_col = next(c for c in schema.columns if c.name == "ok")
        assert ok_col.json_type == "boolean"

    def test_optional_field_not_required(self) -> None:
        from osa.manifest import generate_feature_schema

        schema = generate_feature_schema(AllTypesResult)
        opt_col = next(c for c in schema.columns if c.name == "optional_field")
        assert opt_col.required is False
        assert opt_col.json_type == "string"

    def test_required_field_is_required(self) -> None:
        from osa.manifest import generate_feature_schema

        schema = generate_feature_schema(PocketResult)
        for col in schema.columns:
            assert col.required is True

    def test_list_field_maps_to_array(self) -> None:
        from osa.manifest import generate_feature_schema

        schema = generate_feature_schema(AllTypesResult)
        list_col = next(c for c in schema.columns if c.name == "list_field")
        assert list_col.json_type == "array"

    def test_dict_field_maps_to_object(self) -> None:
        from osa.manifest import generate_feature_schema

        schema = generate_feature_schema(AllTypesResult)
        dict_col = next(c for c in schema.columns if c.name == "dict_field")
        assert dict_col.json_type == "object"

    def test_datetime_gets_date_time_format(self) -> None:
        from osa.manifest import generate_feature_schema

        schema = generate_feature_schema(DateResult)
        created_col = next(c for c in schema.columns if c.name == "created")
        assert created_col.json_type == "string"
        assert created_col.format == "date-time"

    def test_uuid_gets_uuid_format(self) -> None:
        from osa.manifest import generate_feature_schema

        schema = generate_feature_schema(DateResult)
        id_col = next(c for c in schema.columns if c.name == "id")
        assert id_col.json_type == "string"
        assert id_col.format == "uuid"


class TestNewManifestGeneration:
    """Test the updated manifest generation with hooks + conventions."""

    def setup_method(self) -> None:
        from osa._registry import clear

        clear()

    def test_manifest_includes_hooks_with_feature_schema(self) -> None:
        from osa.authoring.hook import hook
        from osa.manifest import generate_manifest

        @hook
        def detect(record: Record[SampleSchema]) -> list[PocketResult]:
            return []

        m = generate_manifest()
        assert len(m.hooks) == 1
        h = m.hooks[0]
        assert h.name == "detect"
        assert h.cardinality == "many"
        assert h.feature_schema is not None
        assert len(h.feature_schema.columns) == 4

    def test_manifest_includes_schemas(self) -> None:
        from osa.authoring.hook import hook
        from osa.manifest import generate_manifest

        @hook
        def detect(record: Record[SampleSchema]) -> list[PocketResult]:
            return []

        m = generate_manifest()
        assert "SampleSchema" in m.schemas

    def test_manifest_serialization_roundtrip(self) -> None:
        from osa.authoring.hook import hook
        from osa.manifest import Manifest, generate_manifest

        @hook
        def detect(record: Record[SampleSchema]) -> list[PocketResult]:
            return []

        m = generate_manifest()
        data = m.model_dump_json()
        restored = Manifest.model_validate_json(data)
        assert len(restored.hooks) == 1
        assert restored.hooks[0].name == "detect"
