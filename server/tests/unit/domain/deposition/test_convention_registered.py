"""Unit tests for enriched ConventionRegistered event.

Tests for User Story 2: Convention Initialization Chain.
Verifies ConventionRegistered carries hooks: list[HookSnapshot].
"""

from uuid import uuid4

from osa.domain.deposition.event.convention_registered import ConventionRegistered
from osa.domain.shared.event import EventId
from osa.domain.shared.model.hook import ColumnDef
from osa.domain.shared.model.hook_snapshot import HookSnapshot
from osa.domain.shared.model.srn import ConventionSRN


def _make_conv_srn() -> ConventionSRN:
    return ConventionSRN.parse("urn:osa:localhost:conv:test@1.0.0")


def _make_hook_snapshot(name: str = "pocket_detect") -> HookSnapshot:
    return HookSnapshot(
        name=name,
        image="ghcr.io/example/hook",
        features=[ColumnDef(name="score", json_type="number", required=True)],
    )


class TestConventionRegisteredWithHooks:
    def test_event_carries_hooks(self):
        """ConventionRegistered carries hooks: list[HookSnapshot]."""
        hooks = [_make_hook_snapshot("hook_a"), _make_hook_snapshot("hook_b")]
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
        hooks = [_make_hook_snapshot()]
        event = ConventionRegistered(
            id=EventId(uuid4()),
            convention_srn=_make_conv_srn(),
            hooks=hooks,
        )

        data = event.model_dump()
        restored = ConventionRegistered.model_validate(data)

        assert len(restored.hooks) == 1
        assert restored.hooks[0].name == "pocket_detect"
        assert restored.hooks[0].image == "ghcr.io/example/hook"
        assert len(restored.hooks[0].features) == 1
