"""Tests for S3IngestStorage adapter."""

from pathlib import Path

import pytest
from botocore.exceptions import ClientError

from osa.infrastructure.s3.ingest_storage import S3IngestStorage
from osa.infrastructure.storage.layout import StorageLayout

DATA_MOUNT = "/data/data"
SRN = "urn:osa:localhost:ing:test-run-001"


def _not_found_error(key: str) -> ClientError:
    return ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": f"Not found: {key}"}},
        "GetObject",
    )


class FakeS3Client:
    """In-memory S3 client for testing."""

    def __init__(self) -> None:
        self._objects: dict[str, bytes] = {}

    async def put_object(self, key: str, body: str | bytes) -> None:
        data = body.encode() if isinstance(body, str) else body
        self._objects[key] = data

    async def get_object(self, key: str) -> bytes:
        if key not in self._objects:
            raise _not_found_error(key)
        return self._objects[key]


@pytest.fixture
def s3() -> FakeS3Client:
    return FakeS3Client()


@pytest.fixture
def storage(s3: FakeS3Client) -> S3IngestStorage:
    layout = StorageLayout(Path(DATA_MOUNT))
    return S3IngestStorage(s3=s3, layout=layout, data_mount_path=DATA_MOUNT)  # type: ignore[arg-type]


class TestReadWriteSession:
    async def test_read_returns_none_when_no_session(self, storage: S3IngestStorage):
        result = await storage.read_session(SRN)
        assert result is None

    async def test_write_then_read_roundtrips(self, storage: S3IngestStorage):
        session = {"offset": 100, "cursor": "abc"}
        await storage.write_session(SRN, session)
        result = await storage.read_session(SRN)
        assert result == session

    async def test_write_overwrites_previous(self, storage: S3IngestStorage):
        await storage.write_session(SRN, {"offset": 100})
        await storage.write_session(SRN, {"offset": 200})
        result = await storage.read_session(SRN)
        assert result == {"offset": 200}

    async def test_write_uses_correct_s3_key(self, storage: S3IngestStorage, s3: FakeS3Client):
        await storage.write_session(SRN, {"x": 1})
        keys = list(s3._objects.keys())
        assert len(keys) == 1
        assert "session.json" in keys[0]
        assert "ingests/" in keys[0]


class TestReadWriteRecords:
    async def test_read_returns_empty_when_no_records(self, storage: S3IngestStorage):
        result = await storage.read_records(SRN, batch_index=0)
        assert result == []

    async def test_write_then_read_roundtrips(self, storage: S3IngestStorage):
        records = [
            {"source_id": "rec1", "metadata": {"title": "Test"}},
            {"source_id": "rec2", "metadata": {"title": "Test 2"}},
        ]
        await storage.write_records(SRN, batch_index=0, records=records)
        result = await storage.read_records(SRN, batch_index=0)
        assert result == records

    async def test_different_batches_are_isolated(self, storage: S3IngestStorage):
        records_0 = [{"source_id": "a", "metadata": {}}]
        records_1 = [{"source_id": "b", "metadata": {}}]
        await storage.write_records(SRN, batch_index=0, records=records_0)
        await storage.write_records(SRN, batch_index=1, records=records_1)
        assert await storage.read_records(SRN, batch_index=0) == records_0
        assert await storage.read_records(SRN, batch_index=1) == records_1

    async def test_write_uses_correct_s3_key(self, storage: S3IngestStorage, s3: FakeS3Client):
        await storage.write_records(
            SRN, batch_index=3, records=[{"source_id": "x", "metadata": {}}]
        )
        keys = list(s3._objects.keys())
        assert len(keys) == 1
        assert "records.jsonl" in keys[0]
        assert "/3/" in keys[0]


class TestPathLocators:
    def test_batch_work_dir_returns_path(self, storage: S3IngestStorage):
        d = storage.batch_work_dir(SRN, batch_index=0)
        assert isinstance(d, Path)
        assert "ingester" in str(d)

    def test_batch_files_dir_returns_path(self, storage: S3IngestStorage):
        d = storage.batch_files_dir(SRN, batch_index=0)
        assert isinstance(d, Path)
        assert str(d).endswith("files")

    def test_hook_work_dir_returns_path(self, storage: S3IngestStorage):
        d = storage.hook_work_dir(SRN, batch_index=0, hook_name="detect_pockets")
        assert isinstance(d, Path)
        assert "detect_pockets" in str(d)

    def test_batch_dir_is_parent_of_work_dir(self, storage: S3IngestStorage):
        bd = storage.batch_dir(SRN, batch_index=0)
        wd = storage.batch_work_dir(SRN, batch_index=0)
        assert wd.parent == bd

    def test_no_mkdir_calls(self, storage: S3IngestStorage):
        """S3 adapter should not create local directories."""
        d = storage.batch_work_dir(SRN, batch_index=99)
        assert not d.exists()
