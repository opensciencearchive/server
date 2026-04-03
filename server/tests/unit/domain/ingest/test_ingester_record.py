"""Tests for IngesterRecord model — from_jsonl parsing and IngesterFileRef."""

import json
from pathlib import Path


def test_from_jsonl_happy_path(tmp_path: Path):
    from osa.domain.ingest.model.ingester_record import IngesterRecord

    records_file = tmp_path / "records.jsonl"
    records_file.write_text(
        json.dumps(
            {
                "source_id": "rec1",
                "metadata": {"title": "Test"},
                "files": [{"name": "f.cif", "relative_path": "rec1/f.cif", "size_mb": 10.5}],
            }
        )
        + "\n"
        + json.dumps({"source_id": "rec2", "metadata": {"title": "Test 2"}})
        + "\n"
    )

    records = IngesterRecord.from_jsonl(records_file)
    assert len(records) == 2
    assert records[0].source_id == "rec1"
    assert records[0].metadata == {"title": "Test"}
    assert len(records[0].files) == 1
    assert records[0].files[0].name == "f.cif"
    assert records[0].files[0].size_mb == 10.5
    assert records[1].source_id == "rec2"
    assert records[1].files == []


def test_from_jsonl_malformed_lines_skipped(tmp_path: Path):
    from osa.domain.ingest.model.ingester_record import IngesterRecord

    records_file = tmp_path / "records.jsonl"
    records_file.write_text(
        "NOT VALID JSON\n" + json.dumps({"source_id": "good", "metadata": {}}) + "\n" + "{broken\n"
    )

    records = IngesterRecord.from_jsonl(records_file)
    assert len(records) == 1
    assert records[0].source_id == "good"


def test_from_jsonl_empty_file(tmp_path: Path):
    from osa.domain.ingest.model.ingester_record import IngesterRecord

    records_file = tmp_path / "records.jsonl"
    records_file.write_text("")
    records = IngesterRecord.from_jsonl(records_file)
    assert records == []


def test_from_jsonl_nonexistent_file(tmp_path: Path):
    from osa.domain.ingest.model.ingester_record import IngesterRecord

    records = IngesterRecord.from_jsonl(tmp_path / "does_not_exist.jsonl")
    assert records == []


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
