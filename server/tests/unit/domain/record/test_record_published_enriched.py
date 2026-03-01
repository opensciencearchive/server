"""Unit tests for enriched RecordPublished event.

Tests for User Story 3: Cross-domain decoupling.
Verifies the event carries convention_srn, hooks, and files_dir.
"""

from uuid import uuid4


from osa.domain.shared.event import EventId
from osa.domain.shared.model.hook import ColumnDef
from osa.domain.shared.model.hook_snapshot import HookSnapshot
from osa.domain.shared.model.srn import ConventionSRN, DepositionSRN, RecordSRN
from osa.domain.record.event.record_published import RecordPublished


def _make_hook_snapshot() -> HookSnapshot:
    return HookSnapshot(
        name="pocketeer",
        image="osa-hooks/pocketeer:latest",
        digest="sha256:abc123",
        features=[ColumnDef(name="score", json_type="number", required=True)],
    )


class TestRecordPublishedEnriched:
    def test_carries_convention_srn(self):
        event = RecordPublished(
            id=EventId(uuid4()),
            record_srn=RecordSRN.parse("urn:osa:localhost:rec:test@1"),
            deposition_srn=DepositionSRN.parse("urn:osa:localhost:dep:test"),
            metadata={"title": "Test"},
            convention_srn=ConventionSRN.parse("urn:osa:localhost:conv:test@1.0.0"),
            hooks=[_make_hook_snapshot()],
            files_dir="/data/files/test-dep",
        )
        assert event.convention_srn == ConventionSRN.parse("urn:osa:localhost:conv:test@1.0.0")

    def test_carries_hooks(self):
        hook = _make_hook_snapshot()
        event = RecordPublished(
            id=EventId(uuid4()),
            record_srn=RecordSRN.parse("urn:osa:localhost:rec:test@1"),
            deposition_srn=DepositionSRN.parse("urn:osa:localhost:dep:test"),
            metadata={"title": "Test"},
            convention_srn=ConventionSRN.parse("urn:osa:localhost:conv:test@1.0.0"),
            hooks=[hook],
            files_dir="/data/files/test-dep",
        )
        assert len(event.hooks) == 1
        assert event.hooks[0].name == "pocketeer"

    def test_carries_files_dir(self):
        event = RecordPublished(
            id=EventId(uuid4()),
            record_srn=RecordSRN.parse("urn:osa:localhost:rec:test@1"),
            deposition_srn=DepositionSRN.parse("urn:osa:localhost:dep:test"),
            metadata={"title": "Test"},
            convention_srn=ConventionSRN.parse("urn:osa:localhost:conv:test@1.0.0"),
            hooks=[],
            files_dir="/data/files/test-dep",
        )
        assert event.files_dir == "/data/files/test-dep"
