"""Unit tests for LocalFileStorageAdapter — path traversal prevention."""

import pytest

from osa.domain.shared.model.srn import DepositionSRN
from osa.infrastructure.persistence.adapter.storage import LocalFileStorageAdapter


def _make_dep_srn() -> DepositionSRN:
    return DepositionSRN.parse("urn:osa:localhost:dep:test-dep-001")


class TestPathTraversalPrevention:
    """Filenames with path traversal components must be rejected."""

    def setup_method(self):
        import tempfile

        self._tmpdir = tempfile.mkdtemp()
        self.adapter = LocalFileStorageAdapter(base_path=self._tmpdir)
        self.dep_srn = _make_dep_srn()

    @pytest.mark.asyncio
    async def test_rejects_parent_directory_traversal(self):
        with pytest.raises(ValueError, match="Invalid filename"):
            await self.adapter.save_file(self.dep_srn, "../../etc/passwd", b"evil", 4)

    @pytest.mark.asyncio
    async def test_rejects_absolute_path(self):
        with pytest.raises(ValueError, match="Invalid filename"):
            await self.adapter.save_file(self.dep_srn, "/etc/passwd", b"evil", 4)

    @pytest.mark.asyncio
    async def test_rejects_dotdot_in_filename(self):
        with pytest.raises(ValueError, match="Invalid filename"):
            await self.adapter.save_file(self.dep_srn, "../secret.csv", b"evil", 4)

    @pytest.mark.asyncio
    async def test_rejects_path_separator_in_filename(self):
        with pytest.raises(ValueError, match="Invalid filename"):
            await self.adapter.save_file(self.dep_srn, "subdir/file.csv", b"data", 4)

    @pytest.mark.asyncio
    async def test_accepts_normal_filename(self):
        result = await self.adapter.save_file(self.dep_srn, "data.csv", b"hello", 5)
        assert result.name == "data.csv"
        assert result.checksum.startswith("sha256:")

    @pytest.mark.asyncio
    async def test_get_file_rejects_traversal(self):
        with pytest.raises(ValueError, match="Invalid filename"):
            await self.adapter.get_file(self.dep_srn, "../../etc/passwd")

    @pytest.mark.asyncio
    async def test_delete_file_rejects_traversal(self):
        with pytest.raises(ValueError, match="Invalid filename"):
            await self.adapter.delete_file(self.dep_srn, "../../etc/passwd")


class TestFileIsolation:
    """get_files_dir returns a files/ subdirectory, not the deposition root."""

    def setup_method(self):
        import tempfile

        self._tmpdir = tempfile.mkdtemp()
        self.adapter = LocalFileStorageAdapter(base_path=self._tmpdir)
        self.dep_srn = _make_dep_srn()

    def test_get_files_dir_returns_files_subdirectory(self):
        files_dir = self.adapter.get_files_dir(self.dep_srn)
        assert files_dir.name == "files"
        assert files_dir.parent == self.adapter._dep_dir(self.dep_srn)

    @pytest.mark.asyncio
    async def test_saved_file_lives_in_files_subdir(self):
        await self.adapter.save_file(self.dep_srn, "data.csv", b"hello", 5)
        expected = self.adapter._dep_dir(self.dep_srn) / "files" / "data.csv"
        assert expected.exists()
