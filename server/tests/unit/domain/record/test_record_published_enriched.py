"""Unit tests for enriched RecordPublished event.

Verifies the event carries source, convention_srn, and expected_features.
"""

from uuid import uuid4

from osa.domain.record.event.record_published import RecordPublished
from osa.domain.shared.event import EventId
from osa.domain.shared.model.source import DepositionSource
from osa.domain.shared.model.srn import ConventionSRN, RecordSRN


class TestRecordPublishedEnriched:
    def test_carries_source(self):
        source = DepositionSource(id="urn:osa:localhost:dep:test")
        event = RecordPublished(
            id=EventId(uuid4()),
            record_srn=RecordSRN.parse("urn:osa:localhost:rec:test@1"),
            source=source,
            metadata={"title": "Test"},
            convention_srn=ConventionSRN.parse("urn:osa:localhost:conv:test@1.0.0"),
            expected_features=["pocketeer"],
        )
        assert event.source.type == "deposition"
        assert event.source.id == "urn:osa:localhost:dep:test"

    def test_carries_convention_srn(self):
        event = RecordPublished(
            id=EventId(uuid4()),
            record_srn=RecordSRN.parse("urn:osa:localhost:rec:test@1"),
            source=DepositionSource(id="urn:osa:localhost:dep:test"),
            metadata={"title": "Test"},
            convention_srn=ConventionSRN.parse("urn:osa:localhost:conv:test@1.0.0"),
            expected_features=[],
        )
        assert event.convention_srn == ConventionSRN.parse("urn:osa:localhost:conv:test@1.0.0")

    def test_carries_expected_features(self):
        event = RecordPublished(
            id=EventId(uuid4()),
            record_srn=RecordSRN.parse("urn:osa:localhost:rec:test@1"),
            source=DepositionSource(id="urn:osa:localhost:dep:test"),
            metadata={"title": "Test"},
            convention_srn=ConventionSRN.parse("urn:osa:localhost:conv:test@1.0.0"),
            expected_features=["pocketeer", "qc_check"],
        )
        assert event.expected_features == ["pocketeer", "qc_check"]
