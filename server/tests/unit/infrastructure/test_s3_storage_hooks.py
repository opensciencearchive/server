"""Tests for S3StorageAdapter hook-log read/write (#147)."""

from collections.abc import AsyncIterator

import pytest

from osa.domain.shared.error import NotFoundError
from osa.infrastructure.s3.storage import S3StorageAdapter

DATA_MOUNT = "/data/data"


class FakeS3Client:
    """Minimal in-memory S3 client exposing the methods read_hook_log uses."""

    def __init__(self) -> None:
        self._objects: dict[str, bytes] = {}

    async def put_object(self, key: str, body: str | bytes) -> None:
        self._objects[key] = body.encode() if isinstance(body, str) else body

    async def head_object(self, key: str) -> bool:
        return key in self._objects

    async def get_object_stream(self, key: str, chunk_size: int = 8192) -> AsyncIterator[bytes]:
        data = self._objects[key]
        for i in range(0, len(data), chunk_size):
            yield data[i : i + chunk_size]


@pytest.fixture
def s3() -> FakeS3Client:
    return FakeS3Client()


@pytest.fixture
def storage(s3: FakeS3Client) -> S3StorageAdapter:
    return S3StorageAdapter(s3=s3, data_mount_path=DATA_MOUNT)  # type: ignore[arg-type]


class TestReadHookLog:
    @pytest.mark.asyncio
    async def test_reads_by_key(self, storage: S3StorageAdapter, s3: FakeS3Client) -> None:
        key = "depositions/localhost_d/hooks/h/output/hook.log"
        await s3.put_object(key, "stderr: kaboom\n")

        stream = await storage.read_hook_log(key)
        data = b"".join([chunk async for chunk in stream])

        assert data == b"stderr: kaboom\n"

    @pytest.mark.asyncio
    async def test_missing_key_raises_not_found(self, storage: S3StorageAdapter) -> None:
        with pytest.raises(NotFoundError):
            await storage.read_hook_log("depositions/d/hooks/h/output/hook.log")

    @pytest.mark.asyncio
    async def test_rejects_traversal_key(self, storage: S3StorageAdapter) -> None:
        with pytest.raises(ValueError, match="Invalid log_ref"):
            await storage.read_hook_log("depositions/../../etc/passwd")
