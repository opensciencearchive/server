"""Tests for osa CLI commands: meta, emit, progress, reject."""

from __future__ import annotations

import json
import os

from pydantic import BaseModel

from osa.types.record import Record
from osa.types.schema import MetadataSchema


class SampleSchema(MetadataSchema):
    organism: str


class PocketResult(BaseModel):
    pocket_id: str
    score: float


class TestOsaMeta:
    def setup_method(self) -> None:
        from osa._registry import clear

        clear()

    def test_meta_outputs_manifest_json(self) -> None:
        from osa.authoring.hook import hook
        from osa.cli.main import meta_command

        @hook
        def detect(record: Record[SampleSchema]) -> list[PocketResult]:
            return []

        output = meta_command()
        data = json.loads(output)
        assert "hooks" in data
        assert "schemas" in data
        assert len(data["hooks"]) == 1
        assert data["hooks"][0]["name"] == "detect"

    def test_meta_includes_feature_schema(self) -> None:
        from osa.authoring.hook import hook
        from osa.cli.main import meta_command

        @hook
        def detect(record: Record[SampleSchema]) -> list[PocketResult]:
            return []

        output = meta_command()
        data = json.loads(output)
        hook_data = data["hooks"][0]
        assert "feature_schema" in hook_data
        assert "columns" in hook_data["feature_schema"]


class TestOsaEmit:
    def test_emit_writes_single_object(self, tmp_path) -> None:
        from osa.cli.main import emit_command

        os.environ["OSA_OUT"] = str(tmp_path)
        try:
            emit_command('{"atom_count": 42}')
            result = json.loads((tmp_path / "features.json").read_text())
            assert result == {"atom_count": 42}
        finally:
            del os.environ["OSA_OUT"]

    def test_emit_writes_array(self, tmp_path) -> None:
        from osa.cli.main import emit_command

        os.environ["OSA_OUT"] = str(tmp_path)
        try:
            emit_command('[{"id": "P1"}, {"id": "P2"}]')
            result = json.loads((tmp_path / "features.json").read_text())
            assert isinstance(result, list)
            assert len(result) == 2
        finally:
            del os.environ["OSA_OUT"]


class TestOsaProgress:
    def test_progress_appends_jsonl(self, tmp_path) -> None:
        from osa.cli.main import progress_command

        os.environ["OSA_OUT"] = str(tmp_path)
        try:
            progress_command(step="Loading", status="running", message="Starting...")
            progress_command(step="Loading", status="completed", message="Done")

            lines = (tmp_path / "progress.jsonl").read_text().strip().split("\n")
            assert len(lines) == 2
            first = json.loads(lines[0])
            assert first["step"] == "Loading"
            assert first["status"] == "running"
        finally:
            del os.environ["OSA_OUT"]


class TestOsaReject:
    def test_reject_writes_rejection(self, tmp_path) -> None:
        from osa.cli.main import reject_command

        os.environ["OSA_OUT"] = str(tmp_path)
        try:
            reject_command(reason="Bad data format")

            lines = (tmp_path / "progress.jsonl").read_text().strip().split("\n")
            assert len(lines) == 1
            data = json.loads(lines[0])
            assert data["status"] == "rejected"
            assert data["reason"] == "Bad data format"
        finally:
            del os.environ["OSA_OUT"]
