import hashlib
import json
import logging
import os
import shutil
import tempfile
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from osa.domain.deposition.model.value import DepositionFile
from osa.domain.deposition.port.storage import FileStoragePort
from osa.domain.shared.error import InfrastructureError
from osa.domain.shared.model.srn import ConventionSRN, DepositionSRN
from osa.domain.validation.model.batch_outcome import (
    BatchRecordOutcome,
    HookRecordId,
    OutcomeStatus,
)

logger = logging.getLogger(__name__)


class FilesystemStorageAdapter(FileStoragePort):
    """Local filesystem adapter satisfying all domain storage ports.

    Implements FileStoragePort (deposition files),
    HookStoragePort, and FeatureStoragePort via structural subtyping.
    """

    def __init__(self, base_path: str) -> None:
        self.base_path = Path(base_path)

    def _dep_dir(self, deposition_id: DepositionSRN) -> Path:
        safe_id = f"{deposition_id.domain.root}_{deposition_id.id.root}"
        return self.base_path / "depositions" / safe_id

    def _files_dir(self, deposition_id: DepositionSRN) -> Path:
        d = self._dep_dir(deposition_id) / "files"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _safe_path(self, base_dir: Path, filename: str) -> Path:
        """Resolve filename within base_dir, rejecting path traversal attempts."""
        safe_name = Path(filename).name
        if not safe_name or safe_name != filename:
            raise ValueError(f"Invalid filename: {filename}")
        target = base_dir / safe_name
        if not target.resolve().is_relative_to(base_dir.resolve()):
            raise ValueError(f"Invalid filename: {filename}")
        return target

    def get_files_dir(self, deposition_id: DepositionSRN) -> Path:
        return self._files_dir(deposition_id)

    def get_hook_output_dir(self, deposition_id: DepositionSRN, hook_name: str) -> Path:
        output_dir = self._dep_dir(deposition_id) / "hooks" / hook_name
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir

    def get_hook_output_root(self, source_type: str, source_id: str) -> str:
        """Resolve the root directory for a given source type and id."""
        if source_type == "deposition":
            srn = DepositionSRN.parse(source_id)
            return str(self._dep_dir(srn))
        if source_type == "ingest":
            return str(self.base_path / "ingests" / source_id)
        raise ValueError(f"Unknown source type: {source_type}")

    async def read_hook_features(
        self, hook_output_dir: str, feature_name: str
    ) -> list[dict[str, Any]]:
        features_file = Path(hook_output_dir) / "hooks" / feature_name / "output" / "features.json"
        if not features_file.exists():
            return []
        data = json.loads(features_file.read_text())
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            return [data]
        return []

    async def hook_features_exist(self, hook_output_dir: str, feature_name: str) -> bool:
        features_file = Path(hook_output_dir) / "hooks" / feature_name / "output" / "features.json"
        return features_file.exists()

    async def save_file(
        self,
        deposition_id: DepositionSRN,
        filename: str,
        content: bytes,
        size: int,
    ) -> DepositionFile:
        files_dir = self._files_dir(deposition_id)
        target = self._safe_path(files_dir, filename)

        # Atomic write: write to temp file then rename (copy+delete on S3 CSI)
        fd, tmp_path = tempfile.mkstemp(dir=files_dir)
        try:
            with open(fd, "wb") as f:
                f.write(content)
            try:
                Path(tmp_path).rename(target)
            except OSError:
                try:
                    shutil.copyfile(tmp_path, target)
                except OSError as e:
                    raise InfrastructureError(f"Failed to write file {filename}: {e}") from e
                try:
                    Path(tmp_path).unlink()
                except OSError:
                    logger.warning("Failed to clean up temp file: %s", tmp_path)
        except Exception:
            Path(tmp_path).unlink(missing_ok=True)
            raise

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
        files_dir = self._files_dir(deposition_id)
        target = self._safe_path(files_dir, filename)
        if not target.exists():
            from osa.domain.shared.error import NotFoundError

            raise NotFoundError(f"File not found: {filename}")

        async def _stream() -> AsyncIterator[bytes]:
            with open(target, "rb") as f:
                while chunk := f.read(8192):
                    yield chunk

        return _stream()

    async def delete_file(
        self,
        deposition_id: DepositionSRN,
        filename: str,
    ) -> None:
        files_dir = self._files_dir(deposition_id)
        target = self._safe_path(files_dir, filename)
        target.unlink(missing_ok=True)

    async def delete_files_for_deposition(
        self,
        deposition_id: DepositionSRN,
    ) -> None:
        dep_dir = self._dep_dir(deposition_id)
        if dep_dir.exists():
            shutil.rmtree(dep_dir)

    def _conv_id(self, convention_srn: ConventionSRN) -> str:
        return f"{convention_srn.domain.root}_{convention_srn.id.root}"

    def get_source_staging_dir(self, convention_srn: ConventionSRN, run_id: str) -> Path:
        staging = self.base_path / "sources" / self._conv_id(convention_srn) / "staging" / run_id
        staging.mkdir(parents=True, exist_ok=True)
        return staging

    def get_source_output_dir(self, convention_srn: ConventionSRN, run_id: str) -> Path:
        output = self.base_path / "sources" / self._conv_id(convention_srn) / "runs" / run_id
        output.mkdir(parents=True, exist_ok=True)
        return output

    async def move_source_files_to_deposition(
        self,
        staging_dir: Path,
        source_id: str,
        deposition_srn: DepositionSRN,
    ) -> None:
        source_files_dir = staging_dir / source_id
        if not source_files_dir.exists():
            return
        files_dir = self._files_dir(deposition_srn)
        # Move files into deposition dir (copy+delete fallback for S3 CSI)
        for f in source_files_dir.iterdir():
            target = files_dir / f.name
            try:
                f.rename(target)
            except OSError:
                try:
                    shutil.copyfile(f, target)
                    f.unlink()
                except OSError as e:
                    raise InfrastructureError(f"Failed to copy file {f.name}: {e}") from e
        # Clean up empty source_id directory
        if source_files_dir.exists():
            source_files_dir.rmdir()

    async def read_batch_outcomes(
        self, output_dir: str, hook_name: str
    ) -> dict[HookRecordId, BatchRecordOutcome]:
        """Read JSONL batch outputs from the filesystem, streaming line-by-line."""
        hook_output = Path(output_dir) / "hooks" / hook_name / "output"
        outcomes: dict[HookRecordId, BatchRecordOutcome] = {}

        _parse_batch_output_files(hook_output, outcomes)

        return outcomes

    async def write_checkpoint(
        self, work_dir: Path, outcomes: dict[HookRecordId, BatchRecordOutcome]
    ) -> None:
        """Atomically write checkpoint JSONL via os.replace()."""
        checkpoint_path = work_dir / "_checkpoint.jsonl"
        tmp_path = work_dir / "_checkpoint.jsonl.tmp"
        with tmp_path.open("w") as f:
            for outcome in outcomes.values():
                f.write(outcome.model_dump_json() + "\n")
        os.replace(tmp_path, checkpoint_path)

    async def write_batch_outcomes(
        self,
        work_dir: Path,
        outcomes: dict[HookRecordId, BatchRecordOutcome],
    ) -> None:
        """Write canonical features.jsonl, rejections.jsonl, errors.jsonl."""
        output_dir = work_dir / "output"
        output_dir.mkdir(parents=True, exist_ok=True)

        features: list[str] = []
        rejections: list[str] = []
        errors: list[str] = []

        for outcome in outcomes.values():
            row: dict[str, Any] = {"id": outcome.record_id}
            if outcome.status == OutcomeStatus.PASSED:
                row["features"] = outcome.features
                features.append(json.dumps(row))
            elif outcome.status == OutcomeStatus.REJECTED:
                row["reason"] = outcome.reason
                rejections.append(json.dumps(row))
            elif outcome.status == OutcomeStatus.ERRORED:
                row["error"] = outcome.error
                row["retryable"] = outcome.retryable
                errors.append(json.dumps(row))

        for filename, lines in [
            ("features.jsonl", features),
            ("rejections.jsonl", rejections),
            ("errors.jsonl", errors),
        ]:
            if lines:
                (output_dir / filename).write_text("\n".join(lines) + "\n")


# ── Shared parsing ──────────────────────────────────────────────────────


_FILE_STATUS_MAP: list[tuple[str, OutcomeStatus, dict[str, str]]] = [
    ("features.jsonl", OutcomeStatus.PASSED, {"features": "features"}),
    ("rejections.jsonl", OutcomeStatus.REJECTED, {"reason": "reason"}),
    ("errors.jsonl", OutcomeStatus.ERRORED, {"error": "error", "retryable": "retryable"}),
]


def _parse_batch_output_files(
    output_dir: Path, outcomes: dict[HookRecordId, BatchRecordOutcome]
) -> None:
    """Parse features/rejections/errors JSONL files into BatchRecordOutcome dict."""
    for filename, status, field_map in _FILE_STATUS_MAP:
        path = output_dir / filename
        if not path.exists():
            continue
        with path.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    logger.warning("Skipping malformed JSON line in %s", filename)
                    continue
                raw_id = data.get("id")
                if not raw_id:
                    logger.warning("Skipping JSONL line without 'id' in %s", filename)
                    continue
                record_id = HookRecordId(raw_id)
                kwargs: dict[str, Any] = {
                    "record_id": record_id,
                    "status": status,
                }
                for src, dst in field_map.items():
                    if src in data:
                        kwargs[dst] = data[src]
                outcomes[record_id] = BatchRecordOutcome(**kwargs)
