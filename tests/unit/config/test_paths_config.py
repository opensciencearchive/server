"""Tests for PathsConfig Pydantic Settings."""

from pathlib import Path

import pytest

from osa.config import Config, PathsConfig


class TestPathsConfig:
    """Tests for PathsConfig Pydantic Settings class."""

    def test_data_dir_defaults_to_none(self) -> None:
        """data_dir should default to None (XDG mode)."""
        config = PathsConfig()
        assert config.data_dir is None

    def test_data_dir_from_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """data_dir should be set from OSA_DATA_DIR environment variable."""
        monkeypatch.setenv("OSA_DATA_DIR", "/data")
        config = PathsConfig()
        assert config.data_dir == Path("/data")

    def test_data_dir_with_absolute_path(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """data_dir should accept absolute paths."""
        monkeypatch.setenv("OSA_DATA_DIR", "/var/lib/osa")
        config = PathsConfig()
        assert config.data_dir == Path("/var/lib/osa")

    def test_data_dir_with_relative_path(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """data_dir should accept relative paths (though not recommended)."""
        monkeypatch.setenv("OSA_DATA_DIR", "./data")
        config = PathsConfig()
        assert config.data_dir == Path("./data")

    def test_env_prefix_is_osa(self) -> None:
        """PathsConfig should use OSA_ prefix for env vars."""
        # Verify the model config
        assert PathsConfig.model_config.get("env_prefix") == "OSA_"


class TestDatabaseUrlDerivation:
    """Tests for database URL derivation from paths (T029-T031)."""

    def test_database_url_derived_from_osa_data_dir(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """T029: Database URL should derive from OSA_DATA_DIR when set."""
        monkeypatch.setenv("OSA_DATA_DIR", "/data")
        # Clear any existing database URL override
        monkeypatch.delenv("OSA_DATABASE__URL", raising=False)

        config = Config()

        # Should derive database path from /data/data/osa.db
        assert config.database.url == "sqlite+aiosqlite:////data/data/osa.db"

    def test_database_url_derived_from_xdg_defaults(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """T030: Database URL should derive from XDG defaults when OSA_DATA_DIR not set."""
        # Clear OSA_DATA_DIR to use XDG mode
        monkeypatch.delenv("OSA_DATA_DIR", raising=False)
        monkeypatch.delenv("OSA_DATABASE__URL", raising=False)

        # Patch Path.home() to use a test path
        test_home = Path("/test/home")
        monkeypatch.setattr(Path, "home", lambda: test_home)

        config = Config()

        # Should derive database path from XDG data directory
        expected_path = test_home / ".local" / "share" / "osa" / "osa.db"
        assert config.database.url == f"sqlite+aiosqlite:///{expected_path}"

    def test_explicit_database_url_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """T031: Explicit OSA_DATABASE__URL should override derived path."""
        # Set both OSA_DATA_DIR and explicit database URL
        monkeypatch.setenv("OSA_DATA_DIR", "/data")
        monkeypatch.setenv("OSA_DATABASE__URL", "postgresql+asyncpg://user:pass@db:5432/osa")

        config = Config()

        # Should use explicit URL, not derived
        assert config.database.url == "postgresql+asyncpg://user:pass@db:5432/osa"

    def test_database_url_preserves_other_settings(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Database URL derivation should preserve echo and auto_migrate settings."""
        monkeypatch.setenv("OSA_DATA_DIR", "/data")
        monkeypatch.delenv("OSA_DATABASE__URL", raising=False)
        monkeypatch.setenv("OSA_DATABASE__ECHO", "true")
        monkeypatch.setenv("OSA_DATABASE__AUTO_MIGRATE", "false")

        config = Config()

        # URL should be derived
        assert "sqlite+aiosqlite" in config.database.url
        # But other settings should be preserved from env vars
        assert config.database.echo is True
        assert config.database.auto_migrate is False
