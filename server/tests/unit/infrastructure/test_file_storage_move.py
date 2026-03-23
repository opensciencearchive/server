"""Tests for FilesystemStorageAdapter move and save fallback behavior.

Tests that move_source_files_to_deposition and save_file fall back to
copy+delete when rename() raises OSError (e.g., cross-device or S3 CSI mount).
"""

from pathlib import Path
from unittest.mock import patch

import pytest

from osa.domain.shared.error import InfrastructureError
from osa.domain.shared.model.srn import DepositionSRN
from osa.infrastructure.persistence.adapter.storage import FilesystemStorageAdapter


def _make_dep_srn() -> DepositionSRN:
    return DepositionSRN.parse("urn:osa:localhost:dep:test123")


class TestMoveSourceFilesFallback:
    """move_source_files_to_deposition falls back to copy+delete on OSError."""

    @pytest.mark.asyncio
    async def test_rename_works_on_local_filesystem(self, tmp_path: Path):
        """rename() still works on local filesystem (no fallback needed)."""
        adapter = FilesystemStorageAdapter(str(tmp_path))
        dep_srn = _make_dep_srn()

        staging_dir = tmp_path / "staging"
        source_id = "src1"
        source_files = staging_dir / source_id
        source_files.mkdir(parents=True)
        (source_files / "data.csv").write_text("a,b,c")

        await adapter.move_source_files_to_deposition(staging_dir, source_id, dep_srn)

        files_dir = adapter.get_files_dir(dep_srn)
        assert (files_dir / "data.csv").read_text() == "a,b,c"
        assert not source_files.exists()

    @pytest.mark.asyncio
    async def test_fallback_copy_delete_on_oserror(self, tmp_path: Path):
        """Falls back to shutil.copyfile + unlink when rename() raises OSError."""
        adapter = FilesystemStorageAdapter(str(tmp_path))
        dep_srn = _make_dep_srn()

        staging_dir = tmp_path / "staging"
        source_id = "src1"
        source_files = staging_dir / source_id
        source_files.mkdir(parents=True)
        (source_files / "data.csv").write_text("a,b,c")

        def failing_rename(self_path, target):
            raise OSError("Cross-device link")

        with patch.object(Path, "rename", failing_rename):
            await adapter.move_source_files_to_deposition(staging_dir, source_id, dep_srn)

        files_dir = adapter.get_files_dir(dep_srn)
        assert (files_dir / "data.csv").read_text() == "a,b,c"
        assert not (source_files / "data.csv").exists()

    @pytest.mark.asyncio
    async def test_fallback_is_idempotent_on_retry(self, tmp_path: Path):
        """Retrying copy+delete after a crash works (file already at target)."""
        adapter = FilesystemStorageAdapter(str(tmp_path))
        dep_srn = _make_dep_srn()

        staging_dir = tmp_path / "staging"
        source_id = "src1"
        source_files = staging_dir / source_id
        source_files.mkdir(parents=True)
        (source_files / "data.csv").write_text("a,b,c")

        # First move (simulating crash after copy but before delete)
        files_dir = adapter.get_files_dir(dep_srn)
        (files_dir / "data.csv").write_text("a,b,c")  # Pre-existing copy

        def failing_rename(self_path, target):
            raise OSError("Cross-device link")

        with patch.object(Path, "rename", failing_rename):
            await adapter.move_source_files_to_deposition(staging_dir, source_id, dep_srn)

        assert (files_dir / "data.csv").read_text() == "a,b,c"
        assert not (source_files / "data.csv").exists()

    @pytest.mark.asyncio
    async def test_copy_failure_raises_infrastructure_error(self, tmp_path: Path):
        """Copy failure wraps OSError in InfrastructureError with file context."""
        adapter = FilesystemStorageAdapter(str(tmp_path))
        dep_srn = _make_dep_srn()

        staging_dir = tmp_path / "staging"
        source_id = "src1"
        source_files = staging_dir / source_id
        source_files.mkdir(parents=True)
        (source_files / "data.csv").write_text("a,b,c")

        def failing_rename(self_path, target):
            raise OSError("Cross-device link")

        with (
            patch.object(Path, "rename", failing_rename),
            patch("shutil.copyfile", side_effect=OSError("No space left on device")),
            pytest.raises(InfrastructureError, match="data.csv"),
        ):
            await adapter.move_source_files_to_deposition(staging_dir, source_id, dep_srn)


class TestSaveFileFallback:
    """save_file atomic write falls back to copy+delete on OSError."""

    @pytest.mark.asyncio
    async def test_save_file_rename_works(self, tmp_path: Path):
        """save_file uses rename for atomic write on local filesystem."""
        adapter = FilesystemStorageAdapter(str(tmp_path))
        dep_srn = _make_dep_srn()
        content = b"hello world"

        result = await adapter.save_file(dep_srn, "test.txt", content, len(content))

        files_dir = adapter.get_files_dir(dep_srn)
        assert (files_dir / "test.txt").read_bytes() == content
        assert result.name == "test.txt"

    @pytest.mark.asyncio
    async def test_save_file_fallback_on_oserror(self, tmp_path: Path):
        """save_file falls back to copy+delete when rename() raises OSError."""
        adapter = FilesystemStorageAdapter(str(tmp_path))
        dep_srn = _make_dep_srn()
        content = b"hello world"

        def failing_rename(self_path, target):
            raise OSError("Cross-device link")

        with patch.object(Path, "rename", failing_rename):
            result = await adapter.save_file(dep_srn, "test.txt", content, len(content))

        files_dir = adapter.get_files_dir(dep_srn)
        assert (files_dir / "test.txt").read_bytes() == content
        assert result.name == "test.txt"

    @pytest.mark.asyncio
    async def test_save_file_copy_failure_raises_infrastructure_error(self, tmp_path: Path):
        """Copy failure wraps OSError in InfrastructureError with filename context."""
        adapter = FilesystemStorageAdapter(str(tmp_path))
        dep_srn = _make_dep_srn()
        content = b"hello world"

        def failing_rename(self_path, target):
            raise OSError("Cross-device link")

        with (
            patch.object(Path, "rename", failing_rename),
            patch(
                "osa.infrastructure.persistence.adapter.storage.shutil.copyfile",
                side_effect=OSError("No space left on device"),
            ),
            pytest.raises(InfrastructureError, match="test.txt"),
        ):
            await adapter.save_file(dep_srn, "test.txt", content, len(content))

    @pytest.mark.asyncio
    async def test_save_file_unlink_failure_after_copy_succeeds(self, tmp_path: Path):
        """If copyfile succeeds but temp unlink fails, the write still succeeds."""
        adapter = FilesystemStorageAdapter(str(tmp_path))
        dep_srn = _make_dep_srn()
        content = b"hello world"

        def failing_rename(self_path, target):
            raise OSError("Cross-device link")

        original_unlink = Path.unlink

        def selective_unlink(self_path, *, missing_ok=False):
            # Only fail for temp files (inside the fallback path)
            if "tmp" in str(self_path) or str(self_path).startswith("/tmp"):
                raise OSError("Permission denied")
            original_unlink(self_path, missing_ok=missing_ok)

        with (
            patch.object(Path, "rename", failing_rename),
            patch.object(Path, "unlink", selective_unlink),
        ):
            result = await adapter.save_file(dep_srn, "test.txt", content, len(content))

        files_dir = adapter.get_files_dir(dep_srn)
        assert (files_dir / "test.txt").read_bytes() == content
        assert result.name == "test.txt"
