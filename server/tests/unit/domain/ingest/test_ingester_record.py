"""Tests for IngesterRecord model — from_dicts parsing and IngesterFileRef."""


def test_from_dicts_happy_path():
    from osa.domain.ingest.model.ingester_record import IngesterRecord

    raw = [
        {
            "source_id": "rec1",
            "metadata": {"title": "Test"},
            "files": [{"name": "f.cif", "relative_path": "rec1/f.cif", "size_mb": 10.5}],
        },
        {"source_id": "rec2", "metadata": {"title": "Test 2"}},
    ]

    records = IngesterRecord.from_dicts(raw)
    assert len(records) == 2
    assert records[0].source_id == "rec1"
    assert records[0].metadata == {"title": "Test"}
    assert len(records[0].files) == 1
    assert records[0].files[0].name == "f.cif"
    assert records[0].files[0].size_mb == 10.5
    assert records[1].source_id == "rec2"
    assert records[1].files == []


def test_from_dicts_malformed_entries_skipped():
    from osa.domain.ingest.model.ingester_record import IngesterRecord

    raw = [
        {"source_id": "good", "metadata": {}},
        {"files": "not_a_list"},  # malformed — missing source_id, bad files
    ]

    records = IngesterRecord.from_dicts(raw)
    assert len(records) >= 1
    assert records[0].source_id == "good"


def test_from_dicts_empty_list():
    from osa.domain.ingest.model.ingester_record import IngesterRecord

    records = IngesterRecord.from_dicts([])
    assert records == []


def test_from_dicts_id_fallback():
    """Records with 'id' instead of 'source_id' should still parse."""
    from osa.domain.ingest.model.ingester_record import IngesterRecord

    raw = [{"id": "fallback-id", "metadata": {"x": 1}}]
    records = IngesterRecord.from_dicts(raw)
    assert len(records) == 1
    assert records[0].source_id == "fallback-id"


def test_total_file_mb_property():
    from osa.domain.ingest.model.ingester_record import IngesterFileRef, IngesterRecord

    record = IngesterRecord(
        source_id="rec1",
        metadata={},
        files=[
            IngesterFileRef(name="a.cif", relative_path="rec1/a.cif", size_mb=10.0),
            IngesterFileRef(name="b.cif", relative_path="rec1/b.cif", size_mb=28.5),
        ],
    )
    assert record.total_file_mb == 38.5


def test_total_file_mb_empty():
    from osa.domain.ingest.model.ingester_record import IngesterRecord

    record = IngesterRecord(source_id="rec1", metadata={})
    assert record.total_file_mb == 0
