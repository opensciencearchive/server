"""S3-backed ingest storage adapter for K8s (cloud) deployments."""

import json
import logging
from pathlib import Path
from typing import Any

from botocore.exceptions import ClientError

from osa.infrastructure.runner_utils import relative_path
from osa.infrastructure.s3.client import S3Client
from osa.infrastructure.storage.layout import StorageLayout

logger = logging.getLogger(__name__)


def _is_not_found(exc: ClientError) -> bool:
    return exc.response["Error"]["Code"] in ("404", "NoSuchKey")


class S3IngestStorage:
    """S3 adapter for IngestStoragePort.

    Used in K8s deployments where the server communicates with S3 natively
    via aioboto3, while container pods mount the same bucket via S3 CSI.

    Path-returning methods return PVC-absolute paths (rooted at data_mount_path)
    used by K8s runners for volume mount subpath computation — no local I/O.
    """

    def __init__(self, s3: S3Client, layout: StorageLayout, data_mount_path: str) -> None:
        self._s3 = s3
        self._layout = layout
        self._data_mount_path = data_mount_path

    def _key(self, path: Path) -> str:
        """Convert a StorageLayout path to an S3 key."""
        return relative_path(path, self._data_mount_path)

    async def read_session(self, ingest_run_srn: str) -> dict[str, Any] | None:
        key = self._key(self._layout.ingest_session_file(ingest_run_srn))
        try:
            data = await self._s3.get_object(key)
            return json.loads(data)
        except ClientError as exc:
            if _is_not_found(exc):
                return None
            raise

    async def write_session(self, ingest_run_srn: str, session: dict[str, Any]) -> None:
        key = self._key(self._layout.ingest_session_file(ingest_run_srn))
        await self._s3.put_object(key, json.dumps(session))

    async def write_records(
        self, ingest_run_srn: str, batch_index: int, records: list[dict[str, Any]]
    ) -> None:
        ingester_dir = self._layout.ingest_batch_ingester_dir(ingest_run_srn, batch_index)
        key = f"{self._key(ingester_dir)}/records.jsonl"
        content = "".join(json.dumps(r) + "\n" for r in records)
        await self._s3.put_object(key, content)

    async def read_records(self, ingest_run_srn: str, batch_index: int) -> list[dict[str, Any]]:
        ingester_dir = self._layout.ingest_batch_ingester_dir(ingest_run_srn, batch_index)
        key = f"{self._key(ingester_dir)}/records.jsonl"
        try:
            data = await self._s3.get_object(key)
        except ClientError as exc:
            if _is_not_found(exc):
                return []
            raise
        records: list[dict[str, Any]] = []
        for line in data.decode().split("\n"):
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
        return records

    def batch_dir(self, ingest_run_srn: str, batch_index: int) -> Path:
        return self._layout.ingest_batch_dir(ingest_run_srn, batch_index)

    def batch_work_dir(self, ingest_run_srn: str, batch_index: int) -> Path:
        return self._layout.ingest_batch_ingester_dir(ingest_run_srn, batch_index)

    def batch_files_dir(self, ingest_run_srn: str, batch_index: int) -> Path:
        return self._layout.ingest_batch_ingester_dir(ingest_run_srn, batch_index) / "files"

    def hook_work_dir(self, ingest_run_srn: str, batch_index: int, hook_name: str) -> Path:
        return self._layout.ingest_batch_hook_dir(ingest_run_srn, batch_index, hook_name)
