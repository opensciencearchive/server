"""Unit tests for enriched DepositionSubmittedEvent.

Verifies the event carries convention_srn and hooks.
"""

from uuid import uuid4

from osa.domain.deposition.event.submitted import DepositionSubmittedEvent
from osa.domain.shared.event import EventId
from osa.domain.shared.model.hook import (
    ColumnDef,
    HookDefinition,
    OciConfig,
    TableFeatureSpec,
)
from osa.domain.shared.model.srn import ConventionSRN, DepositionSRN


def _make_dep_srn() -> DepositionSRN:
    return DepositionSRN.parse("urn:osa:localhost:dep:test-dep")


def _make_conv_srn() -> ConventionSRN:
    return ConventionSRN.parse("urn:osa:localhost:conv:test@1.0.0")


def _make_hook_definition() -> HookDefinition:
    return HookDefinition(
        name="pocketeer",
        runtime=OciConfig(
            image="osa-hooks/pocketeer:latest",
            digest="sha256:abc123",
            config={"threshold": 0.5},
        ),
        feature=TableFeatureSpec(
            cardinality="many",
            columns=[ColumnDef(name="score", json_type="number", required=True)],
        ),
    )


class TestDepositionSubmittedEnriched:
    def test_carries_convention_srn(self):
        """Event has convention_srn field."""
        event = DepositionSubmittedEvent(
            id=EventId(uuid4()),
            deposition_id=_make_dep_srn(),
            metadata={"title": "Test"},
            convention_srn=_make_conv_srn(),
            hooks=[_make_hook_definition()],
        )
        assert event.convention_srn == _make_conv_srn()

    def test_carries_hooks(self):
        """Event has hooks field with HookDefinition list."""
        hook = _make_hook_definition()
        event = DepositionSubmittedEvent(
            id=EventId(uuid4()),
            deposition_id=_make_dep_srn(),
            metadata={"title": "Test"},
            convention_srn=_make_conv_srn(),
            hooks=[hook],
        )
        assert len(event.hooks) == 1
        assert event.hooks[0].name == "pocketeer"
        assert event.hooks[0].runtime.digest == "sha256:abc123"
