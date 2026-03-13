"""Unit tests for project linking (link.py)."""

import json
from pathlib import Path

import pytest

from osa.cli.link import read_link, resolve_server, write_link


class TestWriteLink:
    """Tests for write_link."""

    def test_creates_config_file(self, tmp_path: Path):
        path = write_link("https://archive.example.com", project_dir=tmp_path)

        assert path == tmp_path / ".osa" / "config.json"
        assert path.exists()

        data = json.loads(path.read_text())
        assert data == {"server": "https://archive.example.com"}

    def test_strips_trailing_slash(self, tmp_path: Path):
        write_link("https://example.com///", project_dir=tmp_path)

        data = json.loads((tmp_path / ".osa" / "config.json").read_text())
        assert data["server"] == "https://example.com"

    def test_creates_osa_directory(self, tmp_path: Path):
        write_link("https://example.com", project_dir=tmp_path)
        assert (tmp_path / ".osa").is_dir()

    def test_overwrites_existing_config(self, tmp_path: Path):
        write_link("https://old.com", project_dir=tmp_path)
        write_link("https://new.com", project_dir=tmp_path)

        data = json.loads((tmp_path / ".osa" / "config.json").read_text())
        assert data["server"] == "https://new.com"


class TestReadLink:
    """Tests for read_link."""

    def test_reads_server_url(self, tmp_path: Path):
        write_link("https://example.com", project_dir=tmp_path)

        result = read_link(project_dir=tmp_path)
        assert result == "https://example.com"

    def test_returns_none_when_no_config(self, tmp_path: Path):
        result = read_link(project_dir=tmp_path)
        assert result is None

    def test_returns_none_for_invalid_json(self, tmp_path: Path):
        config_dir = tmp_path / ".osa"
        config_dir.mkdir()
        (config_dir / "config.json").write_text("not json")

        result = read_link(project_dir=tmp_path)
        assert result is None

    def test_returns_none_for_missing_server_key(self, tmp_path: Path):
        config_dir = tmp_path / ".osa"
        config_dir.mkdir()
        (config_dir / "config.json").write_text(json.dumps({"other": "value"}))

        result = read_link(project_dir=tmp_path)
        assert result is None

    def test_returns_none_for_empty_server(self, tmp_path: Path):
        config_dir = tmp_path / ".osa"
        config_dir.mkdir()
        (config_dir / "config.json").write_text(json.dumps({"server": ""}))

        result = read_link(project_dir=tmp_path)
        assert result is None


class TestResolveServer:
    """Tests for resolve_server."""

    def test_flag_takes_highest_priority(self, tmp_path: Path, monkeypatch):
        monkeypatch.setenv("OSA_SERVER", "https://env.com")
        write_link("https://linked.com", project_dir=tmp_path)

        result = resolve_server(flag="https://flag.com", project_dir=tmp_path)
        assert result == "https://flag.com"

    def test_env_takes_priority_over_config(self, tmp_path: Path, monkeypatch):
        monkeypatch.setenv("OSA_SERVER", "https://env.com")
        write_link("https://linked.com", project_dir=tmp_path)

        result = resolve_server(project_dir=tmp_path)
        assert result == "https://env.com"

    def test_config_file_used_when_no_flag_or_env(self, tmp_path: Path, monkeypatch):
        monkeypatch.delenv("OSA_SERVER", raising=False)
        write_link("https://linked.com", project_dir=tmp_path)

        result = resolve_server(project_dir=tmp_path)
        assert result == "https://linked.com"

    def test_exits_when_no_sources(self, tmp_path: Path, monkeypatch):
        monkeypatch.delenv("OSA_SERVER", raising=False)

        with pytest.raises(SystemExit) as exc_info:
            resolve_server(project_dir=tmp_path)

        assert exc_info.value.code == 1

    def test_flag_strips_trailing_slash(self, tmp_path: Path, monkeypatch):
        monkeypatch.delenv("OSA_SERVER", raising=False)

        result = resolve_server(flag="https://example.com/", project_dir=tmp_path)
        assert result == "https://example.com"

    def test_env_strips_trailing_slash(self, tmp_path: Path, monkeypatch):
        monkeypatch.setenv("OSA_SERVER", "https://example.com/")

        result = resolve_server(project_dir=tmp_path)
        assert result == "https://example.com"
