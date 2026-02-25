"""Tests for convention() registration function."""

from __future__ import annotations

from pydantic import BaseModel

from osa.types.record import Record
from osa.types.schema import MetadataSchema


class SampleSchema(MetadataSchema):
    organism: str


class PocketResult(BaseModel):
    pocket_id: str
    score: float


class QualityResult(BaseModel):
    atom_count: int


class TestConventionRegistration:
    def setup_method(self) -> None:
        from osa._registry import clear

        clear()

    def test_convention_records_in_registry(self) -> None:
        from osa._registry import _conventions
        from osa.authoring.convention import convention
        from osa.authoring.hook import hook

        @hook
        def detect(record: Record[SampleSchema]) -> list[PocketResult]:
            return []

        convention(
            title="Test Convention",
            schema=SampleSchema,
            files={"extensions": [".cif"], "min": 1, "max": 10},
            hooks=[detect],
        )
        assert len(_conventions) == 1

    def test_convention_stores_title(self) -> None:
        from osa._registry import _conventions
        from osa.authoring.convention import convention
        from osa.authoring.hook import hook

        @hook
        def detect(record: Record[SampleSchema]) -> list[PocketResult]:
            return []

        convention(
            title="Protein Structure",
            schema=SampleSchema,
            files={"extensions": [".cif"]},
            hooks=[detect],
        )
        assert _conventions[0].title == "Protein Structure"

    def test_convention_stores_schema_type(self) -> None:
        from osa._registry import _conventions
        from osa.authoring.convention import convention
        from osa.authoring.hook import hook

        @hook
        def detect(record: Record[SampleSchema]) -> list[PocketResult]:
            return []

        convention(
            title="Test",
            schema=SampleSchema,
            files={},
            hooks=[detect],
        )
        assert _conventions[0].schema_type is SampleSchema

    def test_convention_stores_file_requirements(self) -> None:
        from osa._registry import _conventions
        from osa.authoring.convention import convention
        from osa.authoring.hook import hook

        @hook
        def detect(record: Record[SampleSchema]) -> list[PocketResult]:
            return []

        files = {"extensions": [".cif", ".pdb"], "min": 1, "max": 10}
        convention(
            title="Test",
            schema=SampleSchema,
            files=files,
            hooks=[detect],
        )
        assert _conventions[0].file_requirements == files

    def test_convention_stores_hook_references(self) -> None:
        from osa._registry import _conventions
        from osa.authoring.convention import convention
        from osa.authoring.hook import hook

        @hook
        def check(record: Record[SampleSchema]) -> QualityResult:
            return QualityResult(atom_count=100)

        @hook
        def detect(record: Record[SampleSchema]) -> list[PocketResult]:
            return []

        convention(
            title="Full",
            schema=SampleSchema,
            files={},
            hooks=[check, detect],
        )
        assert _conventions[0].hooks == [check, detect]

    def test_multiple_conventions(self) -> None:
        from osa._registry import _conventions
        from osa.authoring.convention import convention
        from osa.authoring.hook import hook

        @hook
        def detect(record: Record[SampleSchema]) -> list[PocketResult]:
            return []

        @hook
        def check(record: Record[SampleSchema]) -> QualityResult:
            return QualityResult(atom_count=100)

        convention(
            title="Simple",
            schema=SampleSchema,
            files={},
            hooks=[detect],
        )
        convention(
            title="Detailed",
            schema=SampleSchema,
            files={},
            hooks=[check, detect],
        )
        assert len(_conventions) == 2
        assert _conventions[0].title == "Simple"
        assert _conventions[1].title == "Detailed"
