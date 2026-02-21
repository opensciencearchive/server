"""Tests for the updated runtime entrypoint: handles Reject, writes features.json."""

from __future__ import annotations

import json

from pydantic import BaseModel

from osa.types.record import Record
from osa.types.schema import MetadataSchema


class SampleSchema(MetadataSchema):
    organism: str
    title: str


class PocketResult(BaseModel):
    pocket_id: str
    score: float


class QualityResult(BaseModel):
    atom_count: int


class TestNewEntrypoint:
    def setup_method(self) -> None:
        from osa._registry import clear

        clear()

    def test_success_writes_features_json(self, tmp_path) -> None:
        from osa.authoring.hook import hook
        from osa.runtime.entrypoint import run_hook_entrypoint

        @hook
        def detect(record: Record[SampleSchema]) -> list[PocketResult]:
            return [PocketResult(pocket_id="P1", score=0.85)]

        input_dir = tmp_path / "in"
        output_dir = tmp_path / "out"
        input_dir.mkdir()
        output_dir.mkdir()
        (input_dir / "record.json").write_text(
            json.dumps({"organism": "Human", "title": "Test"})
        )

        exit_code = run_hook_entrypoint(
            hook_fn=detect, input_dir=input_dir, output_dir=output_dir
        )
        assert exit_code == 0

        features = json.loads((output_dir / "features.json").read_text())
        assert isinstance(features, list)
        assert features[0]["pocket_id"] == "P1"

    def test_scalar_result_writes_single_object(self, tmp_path) -> None:
        from osa.authoring.hook import hook
        from osa.runtime.entrypoint import run_hook_entrypoint

        @hook
        def check(record: Record[SampleSchema]) -> QualityResult:
            return QualityResult(atom_count=42)

        input_dir = tmp_path / "in"
        output_dir = tmp_path / "out"
        input_dir.mkdir()
        output_dir.mkdir()
        (input_dir / "record.json").write_text(
            json.dumps({"organism": "Mouse", "title": "Test"})
        )

        exit_code = run_hook_entrypoint(
            hook_fn=check, input_dir=input_dir, output_dir=output_dir
        )
        assert exit_code == 0

        features = json.loads((output_dir / "features.json").read_text())
        assert isinstance(features, dict)
        assert features["atom_count"] == 42

    def test_reject_writes_to_progress(self, tmp_path) -> None:
        from osa.authoring.hook import hook
        from osa.authoring.validator import Reject
        from osa.runtime.entrypoint import run_hook_entrypoint

        @hook
        def check(record: Record[SampleSchema]) -> QualityResult:
            raise Reject("Bad structure file")

        input_dir = tmp_path / "in"
        output_dir = tmp_path / "out"
        input_dir.mkdir()
        output_dir.mkdir()
        (input_dir / "record.json").write_text(
            json.dumps({"organism": "Human", "title": "Test"})
        )

        exit_code = run_hook_entrypoint(
            hook_fn=check, input_dir=input_dir, output_dir=output_dir
        )
        assert exit_code == 0  # Clean exit for rejections

        progress_file = output_dir / "progress.jsonl"
        assert progress_file.exists()
        lines = progress_file.read_text().strip().split("\n")
        data = json.loads(lines[-1])
        assert data["status"] == "rejected"
        assert "Bad structure file" in data["reason"]

    def test_entrypoint_reads_files_dir(self, tmp_path) -> None:
        from osa.authoring.hook import hook
        from osa.runtime.entrypoint import run_hook_entrypoint

        input_dir = tmp_path / "in"
        output_dir = tmp_path / "out"
        input_dir.mkdir()
        output_dir.mkdir()
        files_dir = input_dir / "files"
        files_dir.mkdir()
        (files_dir / "test.cif").write_text("ATOM 1")
        (input_dir / "record.json").write_text(
            json.dumps({"organism": "Human", "title": "Test"})
        )

        @hook
        def detect(record: Record[SampleSchema]) -> list[PocketResult]:
            cif_files = record.files.glob("*.cif")
            assert len(cif_files) == 1
            return [PocketResult(pocket_id="P1", score=0.9)]

        exit_code = run_hook_entrypoint(
            hook_fn=detect, input_dir=input_dir, output_dir=output_dir
        )
        assert exit_code == 0
