"""Unit tests for IndexRecord event with routing_key field.

Tests for User Story 4: Event Routing.
"""

from uuid import uuid4


from osa.domain.index.event.index_record import IndexRecord
from osa.domain.shared.event import EventId
from osa.domain.shared.model.srn import Domain, LocalId, RecordSRN, RecordVersion


class TestIndexRecordRoutingKey:
    """Tests for IndexRecord routing_key field."""

    def test_index_record_has_routing_key_field(self):
        """IndexRecord should have an optional routing_key field."""
        record = IndexRecord(
            id=EventId(uuid4()),
            backend_name="vector",
            record_srn=RecordSRN(
                domain=Domain("test.example.com"),
                id=LocalId("rec-123"),
                version=RecordVersion(1),
            ),
            metadata={"title": "Test"},
            routing_key="vector",
        )

        assert record.routing_key == "vector"

    def test_index_record_routing_key_defaults_to_none(self):
        """IndexRecord routing_key should default to None."""
        record = IndexRecord(
            id=EventId(uuid4()),
            backend_name="vector",
            record_srn=RecordSRN(
                domain=Domain("test.example.com"),
                id=LocalId("rec-123"),
                version=RecordVersion(1),
            ),
            metadata={"title": "Test"},
        )

        assert record.routing_key is None

    def test_index_record_serialization_includes_routing_key(self):
        """IndexRecord serialization should include routing_key."""
        record = IndexRecord(
            id=EventId(uuid4()),
            backend_name="vector",
            record_srn=RecordSRN(
                domain=Domain("test.example.com"),
                id=LocalId("rec-123"),
                version=RecordVersion(1),
            ),
            metadata={"title": "Test"},
            routing_key="vector",
        )

        data = record.model_dump()
        assert "routing_key" in data
        assert data["routing_key"] == "vector"
