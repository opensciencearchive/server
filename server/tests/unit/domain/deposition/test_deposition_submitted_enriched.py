"""Unit tests for enriched DepositionSubmittedEvent.

Verifies the event carries convention_id and hooks (hook **names**, #145 — the
validation handler resolves each name's live release at run start).
"""

from uuid import uuid4

from osa.domain.deposition.event.submitted import DepositionSubmittedEvent
from osa.domain.shared.event import EventId
from osa.domain.shared.model.hook import HookName
from osa.domain.shared.model.srn import ConventionSlug, DepositionSRN


def _make_dep_srn() -> DepositionSRN:
    return DepositionSRN.parse("urn:osa:localhost:dep:test-dep")


def _make_conv_slug() -> ConventionSlug:
    return ConventionSlug("test-conv")


class TestDepositionSubmittedEnriched:
    def test_carries_convention_id(self):
        """Event has convention_id field."""
        event = DepositionSubmittedEvent(
            id=EventId(uuid4()),
            deposition_id=_make_dep_srn(),
            metadata={"title": "Test"},
            convention_id=_make_conv_slug(),
            hooks=[HookName("pocketeer")],
        )
        assert event.convention_id == _make_conv_slug()

    def test_carries_hooks(self):
        """Event has hooks field with HookName list."""
        event = DepositionSubmittedEvent(
            id=EventId(uuid4()),
            deposition_id=_make_dep_srn(),
            metadata={"title": "Test"},
            convention_id=_make_conv_slug(),
            hooks=[HookName("pocketeer")],
        )
        assert len(event.hooks) == 1
        assert event.hooks[0].root == "pocketeer"
