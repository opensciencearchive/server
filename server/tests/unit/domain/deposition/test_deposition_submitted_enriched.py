"""Unit tests for enriched DepositionSubmittedEvent.

Tests for User Story 3: Cross-domain decoupling.
Verifies the event carries convention_srn, hooks, and files_dir.
"""

from uuid import uuid4


from osa.domain.shared.event import EventId
from osa.domain.shared.model.hook import ColumnDef
from osa.domain.shared.model.hook_snapshot import HookSnapshot
from osa.domain.shared.model.srn import ConventionSRN, DepositionSRN
from osa.domain.deposition.event.submitted import DepositionSubmittedEvent


def _make_dep_srn() -> DepositionSRN:
    return DepositionSRN.parse("urn:osa:localhost:dep:test-dep")


def _make_conv_srn() -> ConventionSRN:
    return ConventionSRN.parse("urn:osa:localhost:conv:test@1.0.0")


def _make_hook_snapshot() -> HookSnapshot:
    return HookSnapshot(
        name="pocketeer",
        image="osa-hooks/pocketeer:latest",
        digest="sha256:abc123",
        features=[ColumnDef(name="score", json_type="number", required=True)],
        config={"threshold": 0.5},
    )


class TestDepositionSubmittedEnriched:
    def test_carries_convention_srn(self):
        """Event has convention_srn field."""
        event = DepositionSubmittedEvent(
            id=EventId(uuid4()),
            deposition_id=_make_dep_srn(),
            metadata={"title": "Test"},
            convention_srn=_make_conv_srn(),
            hooks=[_make_hook_snapshot()],
            files_dir="/data/files/test-dep",
        )
        assert event.convention_srn == _make_conv_srn()

    def test_carries_hooks(self):
        """Event has hooks field with HookSnapshot list."""
        hook = _make_hook_snapshot()
        event = DepositionSubmittedEvent(
            id=EventId(uuid4()),
            deposition_id=_make_dep_srn(),
            metadata={"title": "Test"},
            convention_srn=_make_conv_srn(),
            hooks=[hook],
            files_dir="/data/files/test-dep",
        )
        assert len(event.hooks) == 1
        assert event.hooks[0].name == "pocketeer"
        assert event.hooks[0].digest == "sha256:abc123"

    def test_carries_files_dir(self):
        """Event has files_dir field."""
        event = DepositionSubmittedEvent(
            id=EventId(uuid4()),
            deposition_id=_make_dep_srn(),
            metadata={"title": "Test"},
            convention_srn=_make_conv_srn(),
            hooks=[],
            files_dir="/data/files/test-dep",
        )
        assert event.files_dir == "/data/files/test-dep"
