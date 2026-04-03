"""Tests for Config database URL derivation."""

from pathlib import Path

import pytest

from osa.config import Config


@pytest.fixture(autouse=True)
def _config_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure required config env vars are set for all tests."""
    monkeypatch.setenv("OSA_AUTH__JWT__SECRET", "test-secret-key-that-is-at-least-32-chars-long")
    monkeypatch.setenv("OSA_BASE_URL", "http://localhost:8000")


class TestDatabaseUrlDerivation:
    """Tests for database URL derivation from OSAPaths."""

    def test_database_url_derived_from_osa_data_dir(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Database URL should derive from OSA_DATA_DIR when set."""
        monkeypatch.setenv("OSA_DATA_DIR", "/data")
        # Clear any existing database URL override
        monkeypatch.delenv("OSA_DATABASE__URL", raising=False)

        config = Config()

        # Should derive database path from /data/data/osa.db
        assert config.database.url == "sqlite+aiosqlite:////data/data/osa.db"

    def test_database_url_derived_from_xdg_defaults(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Database URL should derive from XDG defaults when OSA_DATA_DIR not set."""
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
        """Explicit OSA_DATABASE__URL should override derived path."""
        # Set both OSA_DATA_DIR and explicit database URL
        monkeypatch.setenv("OSA_DATA_DIR", "/data")
        monkeypatch.setenv("OSA_DATABASE__URL", "postgresql+asyncpg://user:pass@db:5432/osa")

        config = Config()

        # Should use explicit URL, not derived
        assert config.database.url == "postgresql+asyncpg://user:pass@db:5432/osa"

    @pytest.mark.parametrize(
        "input_url",
        [
            "postgresql://user:pass@db:5432/osa",
            "postgres://user:pass@db:5432/osa",
            "postgresql+psycopg2://user:pass@db:5432/osa",
            "postgresql+psycopg://user:pass@db:5432/osa",
            "postgresql+pg8000://user:pass@db:5432/osa",
        ],
    )
    def test_pg_url_normalized_to_asyncpg(
        self, monkeypatch: pytest.MonkeyPatch, input_url: str
    ) -> None:
        """Any PostgreSQL URL variant should be normalized to asyncpg."""
        monkeypatch.setenv("OSA_DATABASE__URL", input_url)

        config = Config()

        assert config.database.url == "postgresql+asyncpg://user:pass@db:5432/osa"

    def test_asyncpg_url_unchanged(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A URL already using asyncpg should not be modified."""
        monkeypatch.setenv("OSA_DATABASE__URL", "postgresql+asyncpg://user:pass@db:5432/osa")

        config = Config()

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
