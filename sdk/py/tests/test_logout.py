"""Unit tests for logout command (T047)."""

from pathlib import Path

import pytest

from osa.cli.credentials import read_credentials, write_credentials
from osa.cli.logout import logout


@pytest.fixture
def cred_file(tmp_path: Path) -> Path:
    return tmp_path / "credentials.json"


class TestLogout:
    """Tests for logout command."""

    def test_removes_credentials(self, cred_file: Path, capsys):
        write_credentials(
            "https://a.com", access_token="at", refresh_token="rt", path=cred_file
        )

        logout("https://a.com", cred_path=cred_file)

        assert read_credentials("https://a.com", path=cred_file) is None
        assert "Logged out" in capsys.readouterr().out

    def test_noop_when_none_exist(self, cred_file: Path, capsys):
        logout("https://a.com", cred_path=cred_file)

        assert "No credentials found" in capsys.readouterr().out

    def test_multi_server_isolation(self, cred_file: Path):
        write_credentials(
            "https://a.com", access_token="a", refresh_token="a", path=cred_file
        )
        write_credentials(
            "https://b.com", access_token="b", refresh_token="b", path=cred_file
        )

        logout("https://a.com", cred_path=cred_file)

        assert read_credentials("https://a.com", path=cred_file) is None
        assert read_credentials("https://b.com", path=cred_file) is not None
