"""Unit tests for ConventionReady event.

Tests for User Story 2: Convention Initialization Chain.
"""

from uuid import uuid4

from osa.domain.feature.event.convention_ready import ConventionReady
from osa.domain.shared.event import EventId
from osa.domain.shared.model.srn import ConventionSRN


def _make_conv_srn() -> ConventionSRN:
    return ConventionSRN.parse("urn:osa:localhost:conv:test-conv@1.0.0")


class TestConventionReady:
    def test_creation_with_convention_srn(self):
        """ConventionReady event carries convention_srn."""
        srn = _make_conv_srn()
        event = ConventionReady(id=EventId(uuid4()), convention_srn=srn)

        assert event.convention_srn == srn
        assert event.id is not None

    def test_serialization_roundtrip(self):
        """ConventionReady serializes and deserializes correctly."""
        srn = _make_conv_srn()
        event = ConventionReady(id=EventId(uuid4()), convention_srn=srn)

        data = event.model_dump()
        restored = ConventionReady.model_validate(data)

        assert restored.convention_srn == event.convention_srn
        assert restored.id == event.id

    def test_registered_in_event_registry(self):
        """ConventionReady should be auto-registered in Event._registry."""
        from osa.domain.shared.event import Event

        assert "ConventionReady" in Event._registry
        assert Event._registry["ConventionReady"] is ConventionReady
