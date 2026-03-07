"""Tests for the shared build_feature_table helper and FeatureSchema model."""

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from osa.domain.shared.model.hook import ColumnDef
from osa.infrastructure.persistence.feature_table import (
    FEATURES_SCHEMA,
    FeatureSchema,
    build_feature_table,
    data_columns,
)


class TestFeatureSchema:
    def test_round_trips_through_json(self) -> None:
        schema = FeatureSchema(
            columns=[
                ColumnDef(name="score", json_type="number", required=True),
                ColumnDef(name="label", json_type="string", required=False),
            ]
        )
        raw = schema.model_dump()
        restored = FeatureSchema.model_validate(raw)
        assert restored == schema

    def test_defaults_to_empty_columns(self) -> None:
        schema = FeatureSchema()
        assert schema.columns == []

    def test_validates_from_catalog_json(self) -> None:
        raw = {
            "columns": [
                {"name": "score", "json_type": "number", "required": True},
                {"name": "label", "json_type": "string", "required": False},
            ]
        }
        schema = FeatureSchema.model_validate(raw)
        assert len(schema.columns) == 2
        assert schema.columns[0].name == "score"
        assert schema.columns[0].json_type == "number"


class TestBuildFeatureTable:
    def test_uses_features_schema(self) -> None:
        table = build_feature_table("my_hook", FeatureSchema())
        assert table.schema == FEATURES_SCHEMA

    def test_has_auto_columns(self) -> None:
        table = build_feature_table("my_hook", FeatureSchema())

        assert "id" in table.c
        assert "record_srn" in table.c
        assert "created_at" in table.c

    def test_id_is_primary_key(self) -> None:
        table = build_feature_table("my_hook", FeatureSchema())
        pk_cols = [c.name for c in table.primary_key.columns]
        assert pk_cols == ["id"]

    def test_record_srn_not_nullable(self) -> None:
        table = build_feature_table("my_hook", FeatureSchema())
        assert not table.c.record_srn.nullable

    def test_data_columns_from_schema(self) -> None:
        schema = FeatureSchema(
            columns=[
                ColumnDef(name="score", json_type="number", required=True),
                ColumnDef(name="label", json_type="string", required=False),
            ]
        )
        table = build_feature_table("detect", schema)

        assert "score" in table.c
        assert isinstance(table.c.score.type, sa.Float)
        assert not table.c.score.nullable

        assert "label" in table.c
        assert isinstance(table.c.label.type, sa.Text)
        assert table.c.label.nullable

    def test_empty_schema_has_only_auto_columns(self) -> None:
        table = build_feature_table("empty", FeatureSchema())
        assert set(c.key for c in table.columns) == {"id", "record_srn", "created_at"}

    def test_array_column_is_jsonb(self) -> None:
        schema = FeatureSchema(
            columns=[ColumnDef(name="residues", json_type="array", required=True)]
        )
        table = build_feature_table("detect", schema)
        assert isinstance(table.c.residues.type, JSONB)

    def test_table_name_matches(self) -> None:
        table = build_feature_table("my_table_name", FeatureSchema())
        assert table.name == "my_table_name"


class TestDataColumns:
    def test_excludes_auto_columns(self) -> None:
        schema = FeatureSchema(
            columns=[
                ColumnDef(name="score", json_type="number", required=True),
                ColumnDef(name="label", json_type="string", required=False),
            ]
        )
        table = build_feature_table("detect", schema)
        dcols = data_columns(table)
        col_names = [c.key for c in dcols]

        assert col_names == ["score", "label"]
        assert "id" not in col_names
        assert "record_srn" not in col_names
        assert "created_at" not in col_names

    def test_empty_for_schema_with_no_data_columns(self) -> None:
        table = build_feature_table("empty", FeatureSchema())
        assert data_columns(table) == []
