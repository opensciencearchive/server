"""Unit tests for enriched ConventionRegistered event.

Tests for User Story 2: Convention Initialization Chain.
Verifies ConventionRegistered carries hooks: list[HookDefinition].
"""

from uuid import uuid4

from osa.domain.deposition.event.convention_registered import ConventionRegistered
from osa.domain.shared.event import EventId
from osa.domain.shared.model.hook import (
    ColumnDef,
    HookDefinition,
    OciConfig,
    TableFeatureSpec,
)
from osa.domain.shared.model.srn import ConventionSRN


def _make_conv_srn() -> ConventionSRN:
    return ConventionSRN.parse("urn:osa:localhost:conv:test@1.0.0")


def _make_hook_definition(name: str = "pocket_detect") -> HookDefinition:
    return HookDefinition(
        name=name,
        runtime=OciConfig(
            image="ghcr.io/example/hook",
            digest="sha256:abc123",
        ),
        feature=TableFeatureSpec(
            cardinality="many",
            columns=[ColumnDef(name="score", json_type="number", required=True)],
        ),
    )


class TestConventionRegisteredWithHooks:
    def test_event_carries_hooks(self):
        """ConventionRegistered carries hooks: list[HookDefinition]."""
        hooks = [_make_hook_definition("hook_a"), _make_hook_definition("hook_b")]
        event = ConventionRegistered(
            id=EventId(uuid4()),
            convention_srn=_make_conv_srn(),
            hooks=hooks,
        )

        assert len(event.hooks) == 2
        assert event.hooks[0].name == "hook_a"
        assert event.hooks[1].name == "hook_b"

    def test_event_defaults_to_empty_hooks(self):
        """ConventionRegistered defaults to empty hooks list."""
        event = ConventionRegistered(
            id=EventId(uuid4()),
            convention_srn=_make_conv_srn(),
        )

        assert event.hooks == []

    def test_serialization_with_hooks(self):
        """ConventionRegistered with hooks serializes and deserializes correctly."""
        hooks = [_make_hook_definition()]
        event = ConventionRegistered(
            id=EventId(uuid4()),
            convention_srn=_make_conv_srn(),
            hooks=hooks,
        )

        data = event.model_dump()
        restored = ConventionRegistered.model_validate(data)

        assert len(restored.hooks) == 1
        assert restored.hooks[0].name == "pocket_detect"
        assert restored.hooks[0].runtime.image == "ghcr.io/example/hook"
        assert len(restored.hooks[0].feature.columns) == 1
