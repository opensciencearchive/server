"""Tests for run_hook() test harness."""

from __future__ import annotations

import pytest
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
    completeness: float


class TestRunHook:
    def setup_method(self) -> None:
        from osa._registry import clear

        clear()

    def test_passes_valid_metadata(self) -> None:
        from osa.authoring.hook import hook
        from osa.testing.harness import run_hook

        @hook
        def check(record: Record[SampleSchema]) -> QualityResult:
            assert record.metadata.organism == "Human"
            return QualityResult(atom_count=100, completeness=0.9)

        result = run_hook(
            check,
            meta={"organism": "Human", "title": "Test"},
        )
        assert result.atom_count == 100

    def test_returns_typed_result_scalar(self) -> None:
        from osa.authoring.hook import hook
        from osa.testing.harness import run_hook

        @hook
        def check(record: Record[SampleSchema]) -> QualityResult:
            return QualityResult(atom_count=42, completeness=0.5)

        result = run_hook(check, meta={"organism": "Mouse", "title": "Test"})
        assert isinstance(result, QualityResult)
        assert result.atom_count == 42

    def test_returns_typed_result_list(self) -> None:
        from osa.authoring.hook import hook
        from osa.testing.harness import run_hook

        @hook
        def detect(record: Record[SampleSchema]) -> list[PocketResult]:
            return [
                PocketResult(pocket_id="P1", score=0.9),
                PocketResult(pocket_id="P2", score=0.7),
            ]

        result = run_hook(detect, meta={"organism": "Human", "title": "Test"})
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0].pocket_id == "P1"

    def test_catches_reject(self) -> None:
        from osa.authoring.hook import hook
        from osa.authoring.validator import Reject
        from osa.testing.harness import run_hook

        @hook
        def check(record: Record[SampleSchema]) -> QualityResult:
            raise Reject("Bad data")

        with pytest.raises(Reject, match="Bad data"):
            run_hook(check, meta={"organism": "Human", "title": "Test"})

    def test_passes_files_directory(self, tmp_path) -> None:
        from osa.authoring.hook import hook
        from osa.testing.harness import run_hook

        # Create a test file
        (tmp_path / "test.cif").write_text("ATOM 1")

        @hook
        def detect(record: Record[SampleSchema]) -> list[PocketResult]:
            files = record.files.glob("*.cif")
            assert len(files) == 1
            return [PocketResult(pocket_id="P1", score=0.8)]

        result = run_hook(
            detect,
            meta={"organism": "Human", "title": "Test"},
            files=tmp_path,
        )
        assert len(result) == 1

    def test_works_without_files(self) -> None:
        from osa.authoring.hook import hook
        from osa.testing.harness import run_hook

        @hook
        def check(record: Record[SampleSchema]) -> QualityResult:
            return QualityResult(atom_count=50, completeness=1.0)

        result = run_hook(check, meta={"organism": "Human", "title": "Test"})
        assert result.completeness == 1.0
