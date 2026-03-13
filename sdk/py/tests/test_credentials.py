"""Unit tests for credentials module (T038)."""

import json
import stat
from pathlib import Path

import pytest

from unittest.mock import patch

from osa.cli.credentials import (
    read_credentials,
    remove_credentials,
    resolve_token,
    write_credentials,
)


@pytest.fixture
def cred_file(tmp_path: Path) -> Path:
    """Return a temporary credentials file path."""
    return tmp_path / "credentials.json"


class TestWriteCredentials:
    """Tests for write_credentials."""

    def test_creates_file_with_tokens(self, cred_file: Path):
        write_credentials(
            "https://archive.example.com",
            access_token="at-123",
            refresh_token="rt-456",
            path=cred_file,
        )

        data = json.loads(cred_file.read_text())
        assert data["https://archive.example.com"]["access_token"] == "at-123"
        assert data["https://archive.example.com"]["refresh_token"] == "rt-456"

    def test_creates_parent_directory(self, tmp_path: Path):
        cred_file = tmp_path / "subdir" / "credentials.json"
        write_credentials(
            "https://example.com",
            access_token="at",
            refresh_token="rt",
            path=cred_file,
        )

        assert cred_file.exists()

    def test_sets_file_permissions_0600(self, cred_file: Path):
        write_credentials(
            "https://example.com",
            access_token="at",
            refresh_token="rt",
            path=cred_file,
        )

        mode = stat.S_IMODE(cred_file.stat().st_mode)
        assert mode == 0o600

    def test_overwrites_existing_server_entry(self, cred_file: Path):
        write_credentials(
            "https://example.com",
            access_token="old",
            refresh_token="old",
            path=cred_file,
        )
        write_credentials(
            "https://example.com",
            access_token="new",
            refresh_token="new",
            path=cred_file,
        )

        data = json.loads(cred_file.read_text())
        assert data["https://example.com"]["access_token"] == "new"

    def test_preserves_other_server_entries(self, cred_file: Path):
        write_credentials(
            "https://server-a.com",
            access_token="a",
            refresh_token="a",
            path=cred_file,
        )
        write_credentials(
            "https://server-b.com",
            access_token="b",
            refresh_token="b",
            path=cred_file,
        )

        data = json.loads(cred_file.read_text())
        assert "https://server-a.com" in data
        assert "https://server-b.com" in data

    def test_normalizes_trailing_slash(self, cred_file: Path):
        write_credentials(
            "https://example.com/",
            access_token="at",
            refresh_token="rt",
            path=cred_file,
        )

        data = json.loads(cred_file.read_text())
        assert "https://example.com" in data
        assert "https://example.com/" not in data


class TestReadCredentials:
    """Tests for read_credentials."""

    def test_returns_tokens_for_known_server(self, cred_file: Path):
        write_credentials(
            "https://example.com",
            access_token="at-123",
            refresh_token="rt-456",
            path=cred_file,
        )

        result = read_credentials("https://example.com", path=cred_file)

        assert result is not None
        assert result["access_token"] == "at-123"
        assert result["refresh_token"] == "rt-456"

    def test_returns_none_for_unknown_server(self, cred_file: Path):
        write_credentials(
            "https://example.com",
            access_token="at",
            refresh_token="rt",
            path=cred_file,
        )

        result = read_credentials("https://other.com", path=cred_file)
        assert result is None

    def test_returns_none_when_file_missing(self, tmp_path: Path):
        result = read_credentials(
            "https://example.com",
            path=tmp_path / "nonexistent.json",
        )
        assert result is None

    def test_normalizes_trailing_slash(self, cred_file: Path):
        write_credentials(
            "https://example.com",
            access_token="at",
            refresh_token="rt",
            path=cred_file,
        )

        result = read_credentials("https://example.com/", path=cred_file)
        assert result is not None


class TestRemoveCredentials:
    """Tests for remove_credentials."""

    def test_removes_server_entry(self, cred_file: Path):
        write_credentials(
            "https://example.com",
            access_token="at",
            refresh_token="rt",
            path=cred_file,
        )

        removed = remove_credentials("https://example.com", path=cred_file)

        assert removed is True
        assert read_credentials("https://example.com", path=cred_file) is None

    def test_returns_false_for_unknown_server(self, cred_file: Path):
        removed = remove_credentials("https://example.com", path=cred_file)
        assert removed is False

    def test_preserves_other_entries(self, cred_file: Path):
        write_credentials(
            "https://a.com", access_token="a", refresh_token="a", path=cred_file
        )
        write_credentials(
            "https://b.com", access_token="b", refresh_token="b", path=cred_file
        )

        remove_credentials("https://a.com", path=cred_file)

        assert read_credentials("https://a.com", path=cred_file) is None
        assert read_credentials("https://b.com", path=cred_file) is not None


class TestResolveToken:
    """Tests for resolve_token credential resolution chain."""

    def test_env_var_takes_precedence(self, cred_file: Path, monkeypatch):
        monkeypatch.setenv("OSA_TOKEN", "env-token")
        write_credentials(
            "https://example.com",
            access_token="stored",
            refresh_token="rt",
            path=cred_file,
        )

        token = resolve_token("https://example.com", path=cred_file)
        assert token == "env-token"

    def test_stored_credentials_used_when_no_env(self, cred_file: Path, monkeypatch):
        monkeypatch.delenv("OSA_TOKEN", raising=False)
        write_credentials(
            "https://example.com",
            access_token="stored-at",
            refresh_token="rt",
            path=cred_file,
        )

        token = resolve_token("https://example.com", path=cred_file)
        assert token == "stored-at"

    def test_returns_none_when_no_credentials(self, cred_file: Path, monkeypatch):
        monkeypatch.delenv("OSA_TOKEN", raising=False)
        token = resolve_token("https://example.com", path=cred_file)
        assert token is None

    def test_attempts_refresh_when_stored_creds_exist(
        self, cred_file: Path, monkeypatch
    ):
        """resolve_token should call refresh_access_token and return refreshed token."""
        monkeypatch.delenv("OSA_TOKEN", raising=False)
        write_credentials(
            "https://example.com",
            access_token="old-at",
            refresh_token="rt",
            path=cred_file,
        )

        with patch(
            "osa.cli.credentials.refresh_access_token", return_value="fresh-at"
        ) as mock_refresh:
            token = resolve_token("https://example.com", path=cred_file)

        assert token == "fresh-at"
        mock_refresh.assert_called_once_with("https://example.com", path=cred_file)

    def test_falls_back_to_stored_token_when_refresh_fails(
        self, cred_file: Path, monkeypatch
    ):
        """resolve_token should return stored token if refresh fails."""
        monkeypatch.delenv("OSA_TOKEN", raising=False)
        write_credentials(
            "https://example.com",
            access_token="stored-at",
            refresh_token="rt",
            path=cred_file,
        )

        with patch("osa.cli.credentials.refresh_access_token", return_value=None):
            token = resolve_token("https://example.com", path=cred_file)

        assert token == "stored-at"
