"""S3 storage adapter — replaces filesystem operations with direct S3 API calls.

Used when runner.backend == "k8s". The OSA server reads/writes S3 natively
via aioboto3 while container pods (hooks/sources) still mount the PVC via S3 CSI.
"""

import hashlib
import json
import logging
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from osa.domain.deposition.model.value import DepositionFile
from osa.domain.deposition.port.storage import FileStoragePort
from osa.domain.shared.error import InfrastructureError, NotFoundError
from osa.domain.shared.model.srn import ConventionSRN, DepositionSRN
from osa.domain.validation.model.batch_outcome import BatchRecordOutcome
from osa.infrastructure.runner_utils import relative_path
from osa.infrastructure.s3.client import S3Client

logger = logging.getLogger(__name__)


class S3StorageAdapter(FileStoragePort):
    """S3-backed adapter satisfying all domain storage ports.

    Implements FileStoragePort, HookStoragePort,
    and FeatureStoragePort via structural subtyping — same as
    FilesystemStorageAdapter but using S3 API instead of POSIX calls.

    Methods that return Path are used only for PVC subpath computation
    by K8s runners (string math, no filesystem I/O).
    """

    def __init__(self, s3: S3Client, data_mount_path: str) -> None:
        self._s3 = s3
        self._data_mount_path = data_mount_path

    # ── Key/path helpers ─────────────────────────────────────────────

    def _safe_id(self, srn: DepositionSRN) -> str:
        return f"{srn.domain.root}_{srn.id.root}"

    def _conv_id(self, convention_srn: ConventionSRN) -> str:
        return f"{convention_srn.domain.root}_{convention_srn.id.root}"

    def _dep_prefix(self, deposition_id: DepositionSRN) -> str:
        return f"depositions/{self._safe_id(deposition_id)}"

    def _files_prefix(self, deposition_id: DepositionSRN) -> str:
        return f"{self._dep_prefix(deposition_id)}/files"

    def _safe_filename(self, filename: str) -> str:
        """Validate filename — reject path traversal attempts."""
        safe_name = Path(filename).name
        if not safe_name or safe_name != filename:
            raise ValueError(f"Invalid filename: {filename}")
        return safe_name

    # ── FileStoragePort ──────────────────────────────────────────────

    def get_files_dir(self, deposition_id: DepositionSRN) -> Path:
        """Return path for PVC subpath computation (no I/O)."""
        safe_id = self._safe_id(deposition_id)
        return Path(self._data_mount_path) / "depositions" / safe_id / "files"

    async def save_file(
        self,
        deposition_id: DepositionSRN,
        filename: str,
        content: bytes,
        size: int,
    ) -> DepositionFile:
        safe_name = self._safe_filename(filename)
        key = f"{self._files_prefix(deposition_id)}/{safe_name}"
        try:
            await self._s3.put_object(key, content)
        except Exception as e:
            raise InfrastructureError(f"Failed to upload file {filename}: {e}") from e

        checksum = hashlib.sha256(content).hexdigest()
        return DepositionFile(
            name=filename,
            size=size,
            checksum=f"sha256:{checksum}",
            content_type=None,
            uploaded_at=datetime.now(UTC),
        )

    async def get_file(
        self,
        deposition_id: DepositionSRN,
        filename: str,
    ) -> AsyncIterator[bytes]:
        safe_name = self._safe_filename(filename)
        key = f"{self._files_prefix(deposition_id)}/{safe_name}"
        if not await self._s3.head_object(key):
            raise NotFoundError(f"File not found: {filename}")
        return self._s3.get_object_stream(key)

    async def delete_file(
        self,
        deposition_id: DepositionSRN,
        filename: str,
    ) -> None:
        safe_name = self._safe_filename(filename)
        key = f"{self._files_prefix(deposition_id)}/{safe_name}"
        await self._s3.delete_object(key)

    async def delete_files_for_deposition(
        self,
        deposition_id: DepositionSRN,
    ) -> None:
        prefix = f"{self._dep_prefix(deposition_id)}/"
        await self._s3.delete_objects(prefix)

    # ── Ingester storage ──────────────────────────────────────────────

    def get_source_staging_dir(self, convention_srn: ConventionSRN, run_id: str) -> Path:
        """Return path for PVC subpath computation (no I/O)."""
        return (
            Path(self._data_mount_path)
            / "sources"
            / self._conv_id(convention_srn)
            / "staging"
            / run_id
        )

    def get_source_output_dir(self, convention_srn: ConventionSRN, run_id: str) -> Path:
        """Return path for PVC subpath computation (no I/O)."""
        return (
            Path(self._data_mount_path)
            / "sources"
            / self._conv_id(convention_srn)
            / "runs"
            / run_id
        )

    async def move_source_files_to_deposition(
        self,
        staging_dir: Path,
        source_id: str,
        deposition_srn: DepositionSRN,
    ) -> None:
        """S3 server-side copy from ingester staging to deposition files prefix."""
        source_prefix = f"{relative_path(staging_dir, self._data_mount_path)}/{source_id}/"

        dest_prefix = self._files_prefix(deposition_srn)

        keys = await self._s3.list_objects(source_prefix)
        for key in keys:
            filename = key.rsplit("/", 1)[-1]
            dest_key = f"{dest_prefix}/{filename}"
            try:
                await self._s3.copy_object(key, dest_key)
                await self._s3.delete_object(key)
            except Exception as e:
                raise InfrastructureError(f"Failed to copy file {filename}: {e}") from e

    # ── HookStoragePort ──────────────────────────────────────────────

    def get_hook_output_dir(self, deposition_id: DepositionSRN, hook_name: str) -> Path:
        """Return path for PVC subpath computation (no I/O)."""
        return (
            Path(self._data_mount_path)
            / "depositions"
            / self._safe_id(deposition_id)
            / "hooks"
            / hook_name
        )

    # ── FeatureStoragePort ───────────────────────────────────────────

    def get_hook_output_root(self, source_type: str, source_id: str) -> str:
        """Resolve the hook output root path for a given source type and id."""
        if source_type == "deposition":
            srn = DepositionSRN.parse(source_id)
            safe_id = self._safe_id(srn)
            return f"{self._data_mount_path}/depositions/{safe_id}"
        if source_type == "ingest":
            return f"{self._data_mount_path}/ingests/{source_id}"
        raise ValueError(f"Unknown source type: {source_type}")

    async def read_hook_features(
        self, hook_output_dir: str, feature_name: str
    ) -> list[dict[str, Any]]:
        prefix = relative_path(Path(hook_output_dir), self._data_mount_path)
        key = f"{prefix}/hooks/{feature_name}/output/features.json"
        try:
            data_bytes = await self._s3.get_object(key)
        except Exception:
            return []
        data = json.loads(data_bytes)
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            return [data]
        return []

    async def hook_features_exist(self, hook_output_dir: str, feature_name: str) -> bool:
        prefix = relative_path(Path(hook_output_dir), self._data_mount_path)
        key = f"{prefix}/hooks/{feature_name}/output/features.json"
        return await self._s3.head_object(key)

    async def read_batch_outcomes(
        self, output_dir: str, hook_name: str
    ) -> dict[str, BatchRecordOutcome]:
        """Read JSONL batch outputs from S3."""
        prefix = relative_path(Path(output_dir), self._data_mount_path)
        hook_prefix = f"{prefix}/hooks/{hook_name}/output"
        outcomes: dict[str, BatchRecordOutcome] = {}

        for filename, status_key, field_map in [
            ("features.jsonl", "passed", {"features": "features"}),
            ("rejections.jsonl", "rejected", {"reason": "reason"}),
            ("errors.jsonl", "errored", {"error": "error", "retryable": "retryable"}),
        ]:
            key = f"{hook_prefix}/{filename}"
            try:
                data_bytes = await self._s3.get_object(key)
            except Exception:
                continue

            for line in data_bytes.decode().split("\n"):
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    logger.warning("Skipping malformed JSON line in %s", filename)
                    continue
                record_id = data.get("id")
                if not record_id:
                    logger.warning("Skipping JSONL line without 'id' in %s", filename)
                    continue
                kwargs: dict[str, Any] = {
                    "record_id": record_id,
                    "status": status_key,
                }
                for src, dst in field_map.items():
                    if src in data:
                        kwargs[dst] = data[src]
                outcomes[record_id] = BatchRecordOutcome(**kwargs)

        return outcomes
