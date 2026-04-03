"""Tests for FilesystemIngestStorage adapter."""

import json
from pathlib import Path

import pytest

from osa.infrastructure.persistence.adapter.ingest_storage import FilesystemIngestStorage
from osa.infrastructure.storage.layout import StorageLayout


@pytest.fixture
def storage(tmp_path: Path) -> FilesystemIngestStorage:
    layout = StorageLayout(tmp_path)
    return FilesystemIngestStorage(layout=layout)


SRN = "urn:osa:localhost:ing:test-run-001"


class TestReadWriteSession:
    async def test_read_returns_none_when_no_session(self, storage: FilesystemIngestStorage):
        result = await storage.read_session(SRN)
        assert result is None

    async def test_write_then_read_roundtrips(self, storage: FilesystemIngestStorage):
        session = {"offset": 100, "cursor": "abc"}
        await storage.write_session(SRN, session)
        result = await storage.read_session(SRN)
        assert result == session

    async def test_write_overwrites_previous_session(self, storage: FilesystemIngestStorage):
        await storage.write_session(SRN, {"offset": 100})
        await storage.write_session(SRN, {"offset": 200})
        result = await storage.read_session(SRN)
        assert result == {"offset": 200}


class TestReadWriteRecords:
    async def test_read_returns_empty_when_no_records(self, storage: FilesystemIngestStorage):
        result = await storage.read_records(SRN, batch_index=0)
        assert result == []

    async def test_write_then_read_roundtrips(self, storage: FilesystemIngestStorage):
        records = [
            {"source_id": "rec1", "metadata": {"title": "Test"}},
            {"source_id": "rec2", "metadata": {"title": "Test 2"}, "files": []},
        ]
        await storage.write_records(SRN, batch_index=0, records=records)
        result = await storage.read_records(SRN, batch_index=0)
        assert result == records

    async def test_different_batches_are_isolated(self, storage: FilesystemIngestStorage):
        records_0 = [{"source_id": "a", "metadata": {}}]
        records_1 = [{"source_id": "b", "metadata": {}}]
        await storage.write_records(SRN, batch_index=0, records=records_0)
        await storage.write_records(SRN, batch_index=1, records=records_1)
        assert await storage.read_records(SRN, batch_index=0) == records_0
        assert await storage.read_records(SRN, batch_index=1) == records_1

    async def test_write_creates_jsonl_file(self, storage: FilesystemIngestStorage):
        records = [{"source_id": "x", "metadata": {}}]
        await storage.write_records(SRN, batch_index=0, records=records)
        work_dir = storage.batch_work_dir(SRN, batch_index=0)
        records_file = work_dir / "records.jsonl"
        assert records_file.exists()
        lines = [line for line in records_file.read_text().splitlines() if line.strip()]
        assert len(lines) == 1
        assert json.loads(lines[0]) == records[0]


class TestPathLocators:
    def test_batch_work_dir_creates_directory(self, storage: FilesystemIngestStorage):
        d = storage.batch_work_dir(SRN, batch_index=0)
        assert d.is_dir()

    def test_batch_files_dir_creates_directory(self, storage: FilesystemIngestStorage):
        d = storage.batch_files_dir(SRN, batch_index=0)
        assert d.is_dir()
        assert d.name == "files"

    def test_hook_work_dir_creates_directory(self, storage: FilesystemIngestStorage):
        d = storage.hook_work_dir(SRN, batch_index=0, hook_name="detect_pockets")
        assert d.is_dir()
        assert "detect_pockets" in str(d)

    def test_batch_dir_creates_directory(self, storage: FilesystemIngestStorage):
        d = storage.batch_dir(SRN, batch_index=0)
        assert d.is_dir()

    def test_batch_dir_is_parent_of_work_dir(self, storage: FilesystemIngestStorage):
        bd = storage.batch_dir(SRN, batch_index=0)
        wd = storage.batch_work_dir(SRN, batch_index=0)
        assert wd.parent == bd
