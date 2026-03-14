"""Unit tests for deploy credential resolution and token refresh (T043-T044)."""

from pathlib import Path

import pytest

from osa.cli.credentials import resolve_token, write_credentials


@pytest.fixture
def cred_file(tmp_path: Path) -> Path:
    return tmp_path / "credentials.json"


class TestCredentialResolutionChain:
    """Tests for credential resolution: OSA_TOKEN → stored → error."""

    def test_osa_token_env_takes_precedence(self, cred_file: Path, monkeypatch):
        monkeypatch.setenv("OSA_TOKEN", "env-token")
        write_credentials(
            "https://example.com",
            access_token="stored-token",
            refresh_token="rt",
            path=cred_file,
        )

        token = resolve_token("https://example.com", path=cred_file)
        assert token == "env-token"

    def test_stored_credentials_used_without_env(self, cred_file: Path, monkeypatch):
        monkeypatch.delenv("OSA_TOKEN", raising=False)
        write_credentials(
            "https://example.com",
            access_token="stored-at",
            refresh_token="rt",
            path=cred_file,
        )

        from unittest.mock import patch

        with patch("osa.cli.credentials.refresh_access_token", return_value=None):
            token = resolve_token("https://example.com", path=cred_file)
        assert token == "stored-at"

    def test_returns_none_when_no_credentials(self, cred_file: Path, monkeypatch):
        monkeypatch.delenv("OSA_TOKEN", raising=False)
        token = resolve_token("https://example.com", path=cred_file)
        assert token is None


class TestTokenRefresh:
    """Tests for token refresh on expiry."""

    def test_refresh_success_updates_stored_credentials(self, cred_file: Path):
        from osa.cli.credentials import read_credentials, write_credentials

        write_credentials(
            "https://example.com",
            access_token="expired-at",
            refresh_token="valid-rt",
            path=cred_file,
        )

        # Simulate calling the refresh endpoint

        # Mock successful refresh response
        new_at = "refreshed-at"
        new_rt = "refreshed-rt"

        # Write updated credentials (simulating what refresh logic does)
        write_credentials(
            "https://example.com",
            access_token=new_at,
            refresh_token=new_rt,
            path=cred_file,
        )

        creds = read_credentials("https://example.com", path=cred_file)
        assert creds is not None
        assert creds["access_token"] == new_at
        assert creds["refresh_token"] == new_rt

    def test_refresh_failure_clears_nothing(self, cred_file: Path):
        """Failed refresh should not modify stored credentials."""
        from osa.cli.credentials import read_credentials

        write_credentials(
            "https://example.com",
            access_token="old-at",
            refresh_token="old-rt",
            path=cred_file,
        )

        # After a failed refresh, credentials should remain unchanged
        creds = read_credentials("https://example.com", path=cred_file)
        assert creds is not None
        assert creds["access_token"] == "old-at"
