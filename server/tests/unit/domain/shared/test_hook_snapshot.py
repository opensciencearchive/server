"""Unit tests for HookSnapshot.from_definitions classmethod."""

from osa.domain.shared.model.hook import (
    ColumnDef,
    FeatureSchema,
    HookDefinition,
    HookManifest,
)
from osa.domain.shared.model.hook_snapshot import HookSnapshot


class TestFromDefinitions:
    """Tests for HookSnapshot.from_definitions()."""

    def test_converts_multiple_hooks(self):
        """from_definitions should convert each HookDefinition to a HookSnapshot."""
        hooks = [
            HookDefinition(
                image="registry.io/hook-a:1.0",
                digest="sha256:aaa",
                manifest=HookManifest(
                    name="hook_a",
                    record_schema="v1",
                    cardinality="one",
                    feature_schema=FeatureSchema(
                        columns=[
                            ColumnDef(name="score", json_type="number", required=True),
                        ]
                    ),
                ),
                config={"threshold": 0.5},
            ),
            HookDefinition(
                image="registry.io/hook-b:2.0",
                digest="sha256:bbb",
                manifest=HookManifest(
                    name="hook_b",
                    record_schema="v1",
                    cardinality="many",
                    feature_schema=FeatureSchema(
                        columns=[
                            ColumnDef(name="label", json_type="string", required=True),
                            ColumnDef(name="count", json_type="integer", required=False),
                        ]
                    ),
                ),
            ),
        ]

        snapshots = HookSnapshot.from_definitions(hooks)

        assert len(snapshots) == 2

        assert snapshots[0].name == "hook_a"
        assert snapshots[0].image == "registry.io/hook-a:1.0"
        assert snapshots[0].digest == "sha256:aaa"
        assert len(snapshots[0].features) == 1
        assert snapshots[0].config == {"threshold": 0.5}

        assert snapshots[1].name == "hook_b"
        assert snapshots[1].image == "registry.io/hook-b:2.0"
        assert snapshots[1].digest == "sha256:bbb"
        assert len(snapshots[1].features) == 2
        assert snapshots[1].config == {}

    def test_empty_list(self):
        """from_definitions should return empty list for empty input."""
        assert HookSnapshot.from_definitions([]) == []
