"""Tests for osa deploy CLI: Dockerfile generation."""

from __future__ import annotations


import pytest


class TestDockerfileGeneration:
    def test_generates_dockerfile_from_pyproject(self, tmp_path) -> None:
        from osa.cli.deploy import generate_dockerfile

        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text(
            '[project]\nname = "test-hooks"\nrequires-python = ">=3.13"\n'
        )

        dockerfile = generate_dockerfile(tmp_path)
        assert "FROM python:3.13-slim" in dockerfile
        assert "pip install" in dockerfile

    def test_dockerfile_uses_python_version_from_pyproject(self, tmp_path) -> None:
        from osa.cli.deploy import generate_dockerfile

        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text(
            '[project]\nname = "test-hooks"\nrequires-python = ">=3.12"\n'
        )

        dockerfile = generate_dockerfile(tmp_path)
        assert "FROM python:3.12-slim" in dockerfile

    def test_dockerfile_includes_entrypoint(self, tmp_path) -> None:
        from osa.cli.deploy import generate_dockerfile

        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text(
            '[project]\nname = "test-hooks"\nrequires-python = ">=3.13"\n'
        )

        dockerfile = generate_dockerfile(tmp_path)
        assert "ENTRYPOINT" in dockerfile
        assert "osa.runtime.entrypoint" in dockerfile

    def test_raises_if_no_pyproject(self, tmp_path) -> None:
        from osa.cli.deploy import generate_dockerfile

        with pytest.raises(FileNotFoundError):
            generate_dockerfile(tmp_path)
