"""Tests for shared hook domain models: HookDefinition, OciConfig, OciLimits, TableFeatureSpec, ColumnDef."""

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


def test_table_feature_spec_with_columns():
    from osa.domain.shared.model.hook import ColumnDef, TableFeatureSpec

    spec = TableFeatureSpec(
        cardinality="many",
        columns=[
            ColumnDef(name="pocket_id", json_type="string", required=True),
            ColumnDef(name="score", json_type="number", required=True),
            ColumnDef(name="volume", json_type="number", required=True),
        ],
    )
    assert len(spec.columns) == 3
    assert spec.columns[0].name == "pocket_id"
    assert spec.kind == "table"
    assert spec.cardinality == "many"


def test_table_feature_spec_empty_columns():
    from osa.domain.shared.model.hook import TableFeatureSpec

    spec = TableFeatureSpec(cardinality="one", columns=[])
    assert spec.columns == []


def test_table_feature_spec_cardinality_one():
    from osa.domain.shared.model.hook import ColumnDef, TableFeatureSpec

    spec = TableFeatureSpec(
        cardinality="one",
        columns=[ColumnDef(name="atom_count", json_type="integer", required=True)],
    )
    assert spec.cardinality == "one"


def test_table_feature_spec_invalid_cardinality():
    from osa.domain.shared.model.hook import TableFeatureSpec

    with pytest.raises(ValidationError):
        TableFeatureSpec(cardinality="invalid", columns=[])


def test_oci_limits_defaults():
    from osa.domain.shared.model.hook import OciLimits

    limits = OciLimits()
    assert limits.timeout_seconds == 300
    assert limits.memory == "1g"
    assert limits.cpu == "0.5"


def test_oci_limits_custom():
    from osa.domain.shared.model.hook import OciLimits

    limits = OciLimits(timeout_seconds=60, memory="512m", cpu="1.0")
    assert limits.timeout_seconds == 60
    assert limits.memory == "512m"
    assert limits.cpu == "1.0"


def test_oci_config_fields():
    from osa.domain.shared.model.hook import OciConfig, OciLimits

    cfg = OciConfig(
        image="ghcr.io/osa/hooks/pocketeer:v1",
        digest="sha256:abc123",
        config={"r_min": 3.0},
        limits=OciLimits(timeout_seconds=120, memory="1g", cpu="1.5"),
    )
    assert cfg.type == "oci"
    assert cfg.image == "ghcr.io/osa/hooks/pocketeer:v1"
    assert cfg.digest == "sha256:abc123"
    assert cfg.config == {"r_min": 3.0}
    assert cfg.limits.timeout_seconds == 120


def test_oci_config_default_config():
    from osa.domain.shared.model.hook import OciConfig

    cfg = OciConfig(image="img:v1", digest="sha256:abc")
    assert cfg.config == {}
    assert cfg.limits.timeout_seconds == 300


def test_hook_definition_full():
    from osa.domain.shared.model.hook import (
        ColumnDef,
        HookDefinition,
        OciConfig,
        OciLimits,
        TableFeatureSpec,
    )

    hook_def = HookDefinition(
        name="detect_pockets",
        runtime=OciConfig(
            image="ghcr.io/osa/hooks/pocketeer:v1",
            digest="sha256:abc123",
            config={"r_min": 3.0},
            limits=OciLimits(timeout_seconds=300, memory="512m", cpu="0.5"),
        ),
        feature=TableFeatureSpec(
            cardinality="many",
            columns=[
                ColumnDef(name="pocket_id", json_type="string", required=True),
                ColumnDef(name="score", json_type="number", required=True),
            ],
        ),
    )
    assert hook_def.name == "detect_pockets"
    assert hook_def.runtime.image == "ghcr.io/osa/hooks/pocketeer:v1"
    assert hook_def.runtime.digest == "sha256:abc123"
    assert hook_def.runtime.config == {"r_min": 3.0}
    assert hook_def.runtime.limits.timeout_seconds == 300
    assert hook_def.feature.cardinality == "many"
    assert len(hook_def.feature.columns) == 2


def test_hook_definition_default_limits():
    from osa.domain.shared.model.hook import (
        HookDefinition,
        OciConfig,
        TableFeatureSpec,
    )

    hook_def = HookDefinition(
        name="h",
        runtime=OciConfig(image="img:v1", digest="sha256:abc"),
        feature=TableFeatureSpec(cardinality="one", columns=[]),
    )
    assert hook_def.runtime.limits.timeout_seconds == 300
    assert hook_def.runtime.limits.memory == "1g"


def test_hook_definition_serialization_roundtrip():
    from osa.domain.shared.model.hook import (
        ColumnDef,
        HookDefinition,
        OciConfig,
        OciLimits,
        TableFeatureSpec,
    )

    hook_def = HookDefinition(
        name="detect_pockets",
        runtime=OciConfig(
            image="ghcr.io/osa/hooks/pocketeer:v1",
            digest="sha256:abc123",
            config={"key": "value"},
            limits=OciLimits(timeout_seconds=120, memory="1g", cpu="1.5"),
        ),
        feature=TableFeatureSpec(
            cardinality="many",
            columns=[
                ColumnDef(name="pocket_id", json_type="string", required=True),
                ColumnDef(name="score", json_type="number", required=False),
            ],
        ),
    )

    data = hook_def.model_dump()
    restored = HookDefinition.model_validate(data)
    assert restored == hook_def
    assert restored.feature.columns[1].required is False


class TestNameValidation:
    """Hook and column names must be safe PG identifiers."""

    def test_hook_name_rejects_uppercase(self):
        from osa.domain.shared.model.hook import HookDefinition, OciConfig, TableFeatureSpec

        with pytest.raises(ValidationError):
            HookDefinition(
                name="BadName",
                runtime=OciConfig(image="img:v1", digest="sha256:abc"),
                feature=TableFeatureSpec(cardinality="one", columns=[]),
            )

    def test_hook_name_rejects_newline_injection(self):
        from osa.domain.shared.model.hook import HookDefinition, OciConfig, TableFeatureSpec

        with pytest.raises(ValidationError):
            HookDefinition(
                name="hook\nEVIL_VAR=pwned",
                runtime=OciConfig(image="img:v1", digest="sha256:abc"),
                feature=TableFeatureSpec(cardinality="one", columns=[]),
            )

    def test_hook_name_rejects_path_traversal(self):
        from osa.domain.shared.model.hook import HookDefinition, OciConfig, TableFeatureSpec

        with pytest.raises(ValidationError):
            HookDefinition(
                name="../etc/passwd",
                runtime=OciConfig(image="img:v1", digest="sha256:abc"),
                feature=TableFeatureSpec(cardinality="one", columns=[]),
            )

    def test_hook_name_rejects_sql_injection(self):
        from osa.domain.shared.model.hook import HookDefinition, OciConfig, TableFeatureSpec

        with pytest.raises(ValidationError):
            HookDefinition(
                name="'; DROP TABLE --",
                runtime=OciConfig(image="img:v1", digest="sha256:abc"),
                feature=TableFeatureSpec(cardinality="one", columns=[]),
            )

    def test_hook_name_rejects_empty(self):
        from osa.domain.shared.model.hook import HookDefinition, OciConfig, TableFeatureSpec

        with pytest.raises(ValidationError):
            HookDefinition(
                name="",
                runtime=OciConfig(image="img:v1", digest="sha256:abc"),
                feature=TableFeatureSpec(cardinality="one", columns=[]),
            )

    def test_hook_name_rejects_leading_digit(self):
        from osa.domain.shared.model.hook import HookDefinition, OciConfig, TableFeatureSpec

        with pytest.raises(ValidationError):
            HookDefinition(
                name="1hook",
                runtime=OciConfig(image="img:v1", digest="sha256:abc"),
                feature=TableFeatureSpec(cardinality="one", columns=[]),
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
        from osa.domain.shared.model.hook import (
            ColumnDef,
            HookDefinition,
            OciConfig,
            TableFeatureSpec,
        )

        valid_names = ["a", "hook_v2", "pocket_detect", "x1", "a_b_c_d"]
        for name in valid_names:
            hook = HookDefinition(
                name=name,
                runtime=OciConfig(image="img:v1", digest="sha256:abc"),
                feature=TableFeatureSpec(cardinality="one", columns=[]),
            )
            assert hook.name == name

            col = ColumnDef(name=name, json_type="number", required=True)
            assert col.name == name
