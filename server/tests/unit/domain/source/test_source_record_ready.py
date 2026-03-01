"""Unit tests for SourceRecordReady event.

Tests for User Story 3: Cross-domain decoupling — source domain.
"""

from uuid import uuid4

from osa.domain.shared.event import EventId
from osa.domain.shared.model.srn import ConventionSRN
from osa.domain.source.event.source_record_ready import SourceRecordReady


def _make_conv_srn() -> ConventionSRN:
    return ConventionSRN.parse("urn:osa:localhost:conv:test@1.0.0")


class TestSourceRecordReady:
    def test_creation_with_all_fields(self):
        """SourceRecordReady carries all required fields."""
        event = SourceRecordReady(
            id=EventId(uuid4()),
            convention_srn=_make_conv_srn(),
            metadata={"pdb_id": "4HHB", "title": "Hemoglobin"},
            file_paths=["4HHB/structure.cif"],
            source_id="4HHB",
            staging_dir="/tmp/staging/run-123",
        )

        assert event.convention_srn == _make_conv_srn()
        assert event.metadata == {"pdb_id": "4HHB", "title": "Hemoglobin"}
        assert event.file_paths == ["4HHB/structure.cif"]
        assert event.source_id == "4HHB"
        assert event.staging_dir == "/tmp/staging/run-123"

    def test_serialization_roundtrip(self):
        """SourceRecordReady serializes and deserializes correctly."""
        event = SourceRecordReady(
            id=EventId(uuid4()),
            convention_srn=_make_conv_srn(),
            metadata={"pdb_id": "1CRN"},
            file_paths=["1CRN/data.cif", "1CRN/meta.json"],
            source_id="1CRN",
            staging_dir="/tmp/staging/run-456",
        )

        data = event.model_dump()
        restored = SourceRecordReady.model_validate(data)

        assert restored.convention_srn == event.convention_srn
        assert restored.metadata == event.metadata
        assert restored.file_paths == event.file_paths
        assert restored.source_id == event.source_id
        assert restored.staging_dir == event.staging_dir

    def test_registered_in_event_registry(self):
        """SourceRecordReady should be auto-registered in Event._registry."""
        from osa.domain.shared.event import Event

        assert "SourceRecordReady" in Event._registry
