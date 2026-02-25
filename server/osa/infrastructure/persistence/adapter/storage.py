import hashlib
import json
import shutil
import tempfile
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from osa.domain.deposition.model.value import DepositionFile
from osa.domain.deposition.port.storage import FileStoragePort
from osa.domain.shared.model.srn import ConventionSRN, DepositionSRN


class LocalFileStorageAdapter(FileStoragePort):
    """Local filesystem implementation of FileStoragePort."""

    def __init__(self, base_path: str) -> None:
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

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

    async def read_hook_features(
        self, deposition_id: DepositionSRN, hook_name: str
    ) -> list[dict[str, Any]]:
        features_file = (
            self._dep_dir(deposition_id) / "hooks" / hook_name / "output" / "features.json"
        )
        if not features_file.exists():
            return []
        data = json.loads(features_file.read_text())
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            return [data]
        return []

    async def hook_features_exist(self, deposition_id: DepositionSRN, hook_name: str) -> bool:
        features_file = (
            self._dep_dir(deposition_id) / "hooks" / hook_name / "output" / "features.json"
        )
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

        # Atomic write: write to temp file then rename
        fd, tmp_path = tempfile.mkstemp(dir=files_dir)
        try:
            with open(fd, "wb") as f:
                f.write(content)
            Path(tmp_path).rename(target)
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

    def move_source_files_to_deposition(
        self,
        staging_dir: Path,
        source_id: str,
        deposition_srn: DepositionSRN,
    ) -> None:
        source_files_dir = staging_dir / source_id
        if not source_files_dir.exists():
            return
        files_dir = self._files_dir(deposition_srn)
        # Rename entire source_id directory contents into deposition files dir
        for f in source_files_dir.iterdir():
            target = files_dir / f.name
            f.rename(target)
        # Clean up empty source_id directory
        source_files_dir.rmdir()
