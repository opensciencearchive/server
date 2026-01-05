"""Tests for OSAPaths path computation."""

from pathlib import Path

import pytest

from osa.cli.util.paths import OSAPaths


class TestOSAPathsXDGMode:
    """Tests for XDG mode (default, when OSA_DATA_DIR is not set)."""

    def test_xdg_mode_uses_home_directory(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """XDG mode should derive paths from home directory."""
        # Ensure OSA_DATA_DIR is not set
        monkeypatch.delenv("OSA_DATA_DIR", raising=False)
        # Patch Path.home() to use a test path
        test_home = Path("/test/home")
        monkeypatch.setattr(Path, "home", lambda: test_home)

        paths = OSAPaths()

        assert paths.config_dir == test_home / ".config" / "osa"
        assert paths.data_dir == test_home / ".local" / "share" / "osa"
        assert paths.state_dir == test_home / ".local" / "state" / "osa"
        assert paths.cache_dir == test_home / ".cache" / "osa"

    def test_xdg_mode_file_paths(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """XDG mode should derive file paths correctly."""
        monkeypatch.delenv("OSA_DATA_DIR", raising=False)
        test_home = Path("/test/home")
        monkeypatch.setattr(Path, "home", lambda: test_home)

        paths = OSAPaths()

        assert paths.config_file == test_home / ".config" / "osa" / "config.yaml"
        assert paths.database_file == test_home / ".local" / "share" / "osa" / "osa.db"
        assert paths.vectors_dir == test_home / ".local" / "share" / "osa" / "vectors"
        assert paths.server_state_file == test_home / ".local" / "state" / "osa" / "server.json"
        assert paths.logs_dir == test_home / ".local" / "state" / "osa" / "logs"
        assert paths.server_log == test_home / ".local" / "state" / "osa" / "logs" / "server.log"
        assert paths.search_cache_file == test_home / ".cache" / "osa" / "last_search.json"


class TestOSAPathsUnifiedMode:
    """Tests for unified mode (when OSA_DATA_DIR is set)."""

    def test_unified_mode_derives_all_paths_from_data_dir(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Unified mode should derive all paths from OSA_DATA_DIR."""
        monkeypatch.setenv("OSA_DATA_DIR", "/data")

        paths = OSAPaths()

        assert paths.config_dir == Path("/data/config")
        assert paths.data_dir == Path("/data/data")
        assert paths.state_dir == Path("/data/state")
        assert paths.cache_dir == Path("/data/cache")

    def test_unified_mode_file_paths(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Unified mode should derive file paths correctly."""
        monkeypatch.setenv("OSA_DATA_DIR", "/data")

        paths = OSAPaths()

        assert paths.config_file == Path("/data/config/config.yaml")
        assert paths.database_file == Path("/data/data/osa.db")
        assert paths.vectors_dir == Path("/data/data/vectors")
        assert paths.server_state_file == Path("/data/state/server.json")
        assert paths.logs_dir == Path("/data/state/logs")
        assert paths.server_log == Path("/data/state/logs/server.log")
        assert paths.search_cache_file == Path("/data/cache/last_search.json")

    def test_unified_mode_with_absolute_path(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Unified mode should work with absolute paths."""
        monkeypatch.setenv("OSA_DATA_DIR", "/var/lib/osa")

        paths = OSAPaths()

        assert paths.data_dir == Path("/var/lib/osa/data")
        assert paths.database_file == Path("/var/lib/osa/data/osa.db")

    def test_unified_mode_subdirectory_structure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Unified mode should create expected subdirectory structure."""
        monkeypatch.setenv("OSA_DATA_DIR", "/data")

        paths = OSAPaths()

        # Verify the structure matches the spec
        assert paths.config_dir.parent == Path("/data")
        assert paths.data_dir.parent == Path("/data")
        assert paths.state_dir.parent == Path("/data")
        assert paths.cache_dir.parent == Path("/data")


class TestOSAPathsEnsureDirectories:
    """Tests for ensure_directories method."""

    def test_ensure_directories_creates_all_dirs(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """ensure_directories should create all required directories."""
        monkeypatch.setenv("OSA_DATA_DIR", str(tmp_path))

        paths = OSAPaths()
        paths.ensure_directories()

        assert paths.config_dir.exists()
        assert paths.data_dir.exists()
        assert paths.state_dir.exists()
        assert paths.cache_dir.exists()
        assert paths.logs_dir.exists()
        assert paths.vectors_dir.exists()

    def test_ensure_directories_is_idempotent(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """ensure_directories should be safe to call multiple times."""
        monkeypatch.setenv("OSA_DATA_DIR", str(tmp_path))

        paths = OSAPaths()
        paths.ensure_directories()
        paths.ensure_directories()  # Should not raise

        assert paths.config_dir.exists()


class TestOSAPathsIsInitialized:
    """Tests for is_initialized method."""

    def test_is_initialized_returns_false_when_no_config(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """is_initialized should return False when config file doesn't exist."""
        monkeypatch.setenv("OSA_DATA_DIR", str(tmp_path))

        paths = OSAPaths()
        assert not paths.is_initialized()

    def test_is_initialized_returns_true_when_config_exists(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """is_initialized should return True when config file exists."""
        monkeypatch.setenv("OSA_DATA_DIR", str(tmp_path))

        paths = OSAPaths()
        paths.ensure_directories()
        paths.config_file.write_text("server:\n  name: test")
        assert paths.is_initialized()
