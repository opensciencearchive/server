"""Tests for OSAPaths path computation."""

from pathlib import Path

import pytest

from osa.cli.util.paths import OSAPaths


class TestOSAPathsXDGMode:
    """Tests for XDG mode (default, when unified_data_dir is None)."""

    def test_xdg_mode_uses_home_directory(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """XDG mode should derive paths from home directory."""
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
    """Tests for unified mode (when unified_data_dir is set)."""

    def test_unified_mode_derives_all_paths_from_data_dir(self) -> None:
        """Unified mode should derive all paths from the single data directory."""
        data_dir = Path("/data")
        paths = OSAPaths(unified_data_dir=data_dir)

        assert paths.config_dir == data_dir / "config"
        assert paths.data_dir == data_dir / "data"
        assert paths.state_dir == data_dir / "state"
        assert paths.cache_dir == data_dir / "cache"

    def test_unified_mode_file_paths(self) -> None:
        """Unified mode should derive file paths correctly."""
        data_dir = Path("/data")
        paths = OSAPaths(unified_data_dir=data_dir)

        assert paths.config_file == data_dir / "config" / "config.yaml"
        assert paths.database_file == data_dir / "data" / "osa.db"
        assert paths.vectors_dir == data_dir / "data" / "vectors"
        assert paths.server_state_file == data_dir / "state" / "server.json"
        assert paths.logs_dir == data_dir / "state" / "logs"
        assert paths.server_log == data_dir / "state" / "logs" / "server.log"
        assert paths.search_cache_file == data_dir / "cache" / "last_search.json"

    def test_unified_mode_with_absolute_path(self) -> None:
        """Unified mode should work with absolute paths."""
        data_dir = Path("/var/lib/osa")
        paths = OSAPaths(unified_data_dir=data_dir)

        assert paths.data_dir == Path("/var/lib/osa/data")
        assert paths.database_file == Path("/var/lib/osa/data/osa.db")

    def test_unified_mode_subdirectory_structure(self) -> None:
        """Unified mode should create expected subdirectory structure."""
        data_dir = Path("/data")
        paths = OSAPaths(unified_data_dir=data_dir)

        # Verify the structure matches the spec
        assert paths.config_dir.parent == data_dir
        assert paths.data_dir.parent == data_dir
        assert paths.state_dir.parent == data_dir
        assert paths.cache_dir.parent == data_dir


class TestOSAPathsEnsureDirectories:
    """Tests for ensure_directories method."""

    def test_ensure_directories_creates_all_dirs(self, tmp_path: Path) -> None:
        """ensure_directories should create all required directories."""
        paths = OSAPaths(unified_data_dir=tmp_path)
        paths.ensure_directories()

        assert paths.config_dir.exists()
        assert paths.data_dir.exists()
        assert paths.state_dir.exists()
        assert paths.cache_dir.exists()
        assert paths.logs_dir.exists()
        assert paths.vectors_dir.exists()

    def test_ensure_directories_is_idempotent(self, tmp_path: Path) -> None:
        """ensure_directories should be safe to call multiple times."""
        paths = OSAPaths(unified_data_dir=tmp_path)
        paths.ensure_directories()
        paths.ensure_directories()  # Should not raise

        assert paths.config_dir.exists()


class TestOSAPathsIsInitialized:
    """Tests for is_initialized method."""

    def test_is_initialized_returns_false_when_no_config(self, tmp_path: Path) -> None:
        """is_initialized should return False when config file doesn't exist."""
        paths = OSAPaths(unified_data_dir=tmp_path)
        assert not paths.is_initialized()

    def test_is_initialized_returns_true_when_config_exists(self, tmp_path: Path) -> None:
        """is_initialized should return True when config file exists."""
        paths = OSAPaths(unified_data_dir=tmp_path)
        paths.ensure_directories()
        paths.config_file.write_text("server:\n  name: test")
        assert paths.is_initialized()
