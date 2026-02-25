"""Tests for shared hook domain models: HookDefinition, HookLimits, HookManifest, FeatureSchema, ColumnDef."""

import pytest
from pydantic import ValidationError


def test_column_def_required_fields():
    from osa.domain.shared.model.hook import ColumnDef

    col = ColumnDef(name="score", json_type="number", required=True)
    assert col.name == "score"
    assert col.json_type == "number"
    assert col.format is None
    assert col.required is True


def test_column_def_with_format():
    from osa.domain.shared.model.hook import ColumnDef

    col = ColumnDef(name="created", json_type="string", format="date-time", required=False)
    assert col.format == "date-time"
    assert col.required is False


def test_column_def_all_json_types():
    from osa.domain.shared.model.hook import ColumnDef

    valid_types = ["string", "number", "integer", "boolean", "array", "object"]
    for jt in valid_types:
        col = ColumnDef(name="x", json_type=jt, required=True)
        assert col.json_type == jt


def test_column_def_invalid_json_type():
    from osa.domain.shared.model.hook import ColumnDef

    with pytest.raises(ValidationError):
        ColumnDef(name="x", json_type="invalid", required=True)


def test_column_def_is_frozen():
    from osa.domain.shared.model.hook import ColumnDef

    col = ColumnDef(name="x", json_type="string", required=True)
    with pytest.raises(ValidationError):
        col.name = "y"


def test_feature_schema_with_columns():
    from osa.domain.shared.model.hook import ColumnDef, FeatureSchema

    schema = FeatureSchema(
        columns=[
            ColumnDef(name="pocket_id", json_type="string", required=True),
            ColumnDef(name="score", json_type="number", required=True),
            ColumnDef(name="volume", json_type="number", required=True),
        ]
    )
    assert len(schema.columns) == 3
    assert schema.columns[0].name == "pocket_id"


def test_feature_schema_empty_columns():
    from osa.domain.shared.model.hook import FeatureSchema

    schema = FeatureSchema(columns=[])
    assert schema.columns == []


def test_hook_manifest_fields():
    from osa.domain.shared.model.hook import ColumnDef, FeatureSchema, HookManifest

    manifest = HookManifest(
        name="detect_pockets",
        record_schema="ProteinStructure",
        cardinality="many",
        feature_schema=FeatureSchema(
            columns=[ColumnDef(name="pocket_id", json_type="string", required=True)]
        ),
        runner="oci",
    )
    assert manifest.name == "detect_pockets"
    assert manifest.record_schema == "ProteinStructure"
    assert manifest.cardinality == "many"
    assert manifest.runner == "oci"
    assert len(manifest.feature_schema.columns) == 1


def test_hook_manifest_cardinality_one():
    from osa.domain.shared.model.hook import ColumnDef, FeatureSchema, HookManifest

    manifest = HookManifest(
        name="structure_check",
        record_schema="ProteinStructure",
        cardinality="one",
        feature_schema=FeatureSchema(
            columns=[ColumnDef(name="atom_count", json_type="integer", required=True)]
        ),
        runner="oci",
    )
    assert manifest.cardinality == "one"


def test_hook_manifest_invalid_cardinality():
    from osa.domain.shared.model.hook import FeatureSchema, HookManifest

    with pytest.raises(ValidationError):
        HookManifest(
            name="bad",
            record_schema="X",
            cardinality="invalid",
            feature_schema=FeatureSchema(columns=[]),
            runner="oci",
        )


def test_hook_limits_defaults():
    from osa.domain.shared.model.hook import HookLimits

    limits = HookLimits()
    assert limits.timeout_seconds == 300
    assert limits.memory == "2g"
    assert limits.cpu == "2.0"


def test_hook_limits_custom():
    from osa.domain.shared.model.hook import HookLimits

    limits = HookLimits(timeout_seconds=60, memory="512m", cpu="1.0")
    assert limits.timeout_seconds == 60
    assert limits.memory == "512m"
    assert limits.cpu == "1.0"


def test_hook_definition_full():
    from osa.domain.shared.model.hook import (
        ColumnDef,
        FeatureSchema,
        HookDefinition,
        HookLimits,
        HookManifest,
    )

    hook_def = HookDefinition(
        image="ghcr.io/osa/hooks/pocketeer:v1",
        digest="sha256:abc123",
        runner="oci",
        config={"r_min": 3.0},
        limits=HookLimits(timeout_seconds=300, memory="2g", cpu="2.0"),
        manifest=HookManifest(
            name="detect_pockets",
            record_schema="ProteinStructure",
            cardinality="many",
            feature_schema=FeatureSchema(
                columns=[
                    ColumnDef(name="pocket_id", json_type="string", required=True),
                    ColumnDef(name="score", json_type="number", required=True),
                ]
            ),
            runner="oci",
        ),
    )
    assert hook_def.image == "ghcr.io/osa/hooks/pocketeer:v1"
    assert hook_def.digest == "sha256:abc123"
    assert hook_def.config == {"r_min": 3.0}
    assert hook_def.manifest.name == "detect_pockets"
    assert hook_def.limits.timeout_seconds == 300


def test_hook_definition_no_config():
    from osa.domain.shared.model.hook import (
        ColumnDef,
        FeatureSchema,
        HookDefinition,
        HookLimits,
        HookManifest,
    )

    hook_def = HookDefinition(
        image="ghcr.io/osa/hooks/check:v1",
        digest="sha256:def456",
        runner="oci",
        config=None,
        limits=HookLimits(),
        manifest=HookManifest(
            name="check",
            record_schema="X",
            cardinality="one",
            feature_schema=FeatureSchema(
                columns=[ColumnDef(name="ok", json_type="boolean", required=True)]
            ),
            runner="oci",
        ),
    )
    assert hook_def.config is None


def test_hook_definition_default_limits():
    from osa.domain.shared.model.hook import (
        FeatureSchema,
        HookDefinition,
        HookManifest,
    )

    hook_def = HookDefinition(
        image="img:v1",
        digest="sha256:abc",
        runner="oci",
        manifest=HookManifest(
            name="h",
            record_schema="S",
            cardinality="one",
            feature_schema=FeatureSchema(columns=[]),
            runner="oci",
        ),
    )
    assert hook_def.limits.timeout_seconds == 300
    assert hook_def.limits.memory == "2g"


def test_hook_definition_serialization_roundtrip():
    from osa.domain.shared.model.hook import (
        ColumnDef,
        FeatureSchema,
        HookDefinition,
        HookLimits,
        HookManifest,
    )

    hook_def = HookDefinition(
        image="ghcr.io/osa/hooks/pocketeer:v1",
        digest="sha256:abc123",
        runner="oci",
        config={"key": "value"},
        limits=HookLimits(timeout_seconds=120, memory="1g", cpu="1.5"),
        manifest=HookManifest(
            name="detect_pockets",
            record_schema="ProteinStructure",
            cardinality="many",
            feature_schema=FeatureSchema(
                columns=[
                    ColumnDef(name="pocket_id", json_type="string", required=True),
                    ColumnDef(name="score", json_type="number", required=False),
                ]
            ),
            runner="oci",
        ),
    )

    data = hook_def.model_dump()
    restored = HookDefinition.model_validate(data)
    assert restored == hook_def
    assert restored.manifest.feature_schema.columns[1].required is False


class TestNameValidation:
    """Hook and column names must be safe PG identifiers."""

    def test_manifest_name_rejects_uppercase(self):
        from osa.domain.shared.model.hook import FeatureSchema, HookManifest

        with pytest.raises(ValidationError):
            HookManifest(
                name="BadName",
                record_schema="S",
                cardinality="one",
                feature_schema=FeatureSchema(columns=[]),
            )

    def test_manifest_name_rejects_newline_injection(self):
        from osa.domain.shared.model.hook import FeatureSchema, HookManifest

        with pytest.raises(ValidationError):
            HookManifest(
                name="hook\nEVIL_VAR=pwned",
                record_schema="S",
                cardinality="one",
                feature_schema=FeatureSchema(columns=[]),
            )

    def test_manifest_name_rejects_path_traversal(self):
        from osa.domain.shared.model.hook import FeatureSchema, HookManifest

        with pytest.raises(ValidationError):
            HookManifest(
                name="../etc/passwd",
                record_schema="S",
                cardinality="one",
                feature_schema=FeatureSchema(columns=[]),
            )

    def test_manifest_name_rejects_sql_injection(self):
        from osa.domain.shared.model.hook import FeatureSchema, HookManifest

        with pytest.raises(ValidationError):
            HookManifest(
                name="'; DROP TABLE --",
                record_schema="S",
                cardinality="one",
                feature_schema=FeatureSchema(columns=[]),
            )

    def test_manifest_name_rejects_empty(self):
        from osa.domain.shared.model.hook import FeatureSchema, HookManifest

        with pytest.raises(ValidationError):
            HookManifest(
                name="",
                record_schema="S",
                cardinality="one",
                feature_schema=FeatureSchema(columns=[]),
            )

    def test_manifest_name_rejects_leading_digit(self):
        from osa.domain.shared.model.hook import FeatureSchema, HookManifest

        with pytest.raises(ValidationError):
            HookManifest(
                name="1hook",
                record_schema="S",
                cardinality="one",
                feature_schema=FeatureSchema(columns=[]),
            )

    def test_column_name_rejects_unsafe_input(self):
        from osa.domain.shared.model.hook import ColumnDef

        with pytest.raises(ValidationError):
            ColumnDef(name="'; DROP TABLE --", json_type="number", required=True)

    def test_column_name_rejects_spaces(self):
        from osa.domain.shared.model.hook import ColumnDef

        with pytest.raises(ValidationError):
            ColumnDef(name="my column", json_type="number", required=True)

    def test_valid_names_accepted(self):
        from osa.domain.shared.model.hook import ColumnDef, FeatureSchema, HookManifest

        valid_names = ["a", "hook_v2", "pocket_detect", "x1", "a_b_c_d"]
        for name in valid_names:
            manifest = HookManifest(
                name=name,
                record_schema="S",
                cardinality="one",
                feature_schema=FeatureSchema(columns=[]),
            )
            assert manifest.name == name

            col = ColumnDef(name=name, json_type="number", required=True)
            assert col.name == name
