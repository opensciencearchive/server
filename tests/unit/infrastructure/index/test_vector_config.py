"""Tests for vector index configuration with OSA_DATA_DIR support."""

from pathlib import Path

import pytest

from osa.cli.util.paths import OSAPaths
from osa.infrastructure.index.vector.config import VectorBackendConfig


class TestVectorBackendConfig:
    """Tests for VectorBackendConfig persist_dir handling."""

    def test_persist_dir_defaults_to_none(self) -> None:
        """persist_dir should default to None (derive from OSAPaths)."""
        config = VectorBackendConfig()
        assert config.persist_dir is None

    def test_persist_dir_accepts_explicit_path(self) -> None:
        """persist_dir should accept explicit path values."""
        config = VectorBackendConfig(persist_dir=Path("/custom/vectors"))
        assert config.persist_dir == Path("/custom/vectors")


class TestVectorPersistDirDerivation:
    """Tests for vector persist_dir derivation from OSAPaths."""

    def test_persist_dir_derived_from_osa_data_dir(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Vector persist_dir should derive from OSA_DATA_DIR when set."""
        monkeypatch.setenv("OSA_DATA_DIR", "/data")

        paths = OSAPaths()

        # Verify paths derives vectors_dir correctly
        assert paths.vectors_dir == Path("/data/data/vectors")

    def test_persist_dir_derived_from_xdg_defaults(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Vector persist_dir should derive from XDG defaults when OSA_DATA_DIR not set."""
        monkeypatch.delenv("OSA_DATA_DIR", raising=False)
        # Patch Path.home() to use a test path
        test_home = Path("/test/home")
        monkeypatch.setattr(Path, "home", lambda: test_home)

        paths = OSAPaths()

        # Should derive vectors path from XDG data directory
        expected_path = test_home / ".local" / "share" / "osa" / "vectors"
        assert paths.vectors_dir == expected_path

    def test_persist_dir_with_tmp_path(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Vector persist_dir should work with actual tmp directories."""
        monkeypatch.setenv("OSA_DATA_DIR", str(tmp_path))

        paths = OSAPaths()
        paths.ensure_directories()

        # Verify the vectors directory exists
        assert paths.vectors_dir.exists()
        assert paths.vectors_dir == tmp_path / "data" / "vectors"
