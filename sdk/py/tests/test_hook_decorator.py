"""Tests for the unified @hook decorator: registration, type introspection, cardinality detection."""

from __future__ import annotations

from pydantic import BaseModel

from osa.types.record import Record
from osa.types.schema import MetadataSchema


class SampleSchema(MetadataSchema):
    organism: str
    resolution: float | None = None


class PocketResult(BaseModel):
    pocket_id: str
    score: float


class QualityResult(BaseModel):
    atom_count: int
    completeness: float


class TestHookRegistration:
    def setup_method(self) -> None:
        from osa._registry import clear

        clear()

    def test_hook_registers_in_global_registry(self) -> None:
        from osa._registry import _hooks
        from osa.authoring.hook import hook

        @hook
        def detect_pockets(record: Record[SampleSchema]) -> list[PocketResult]:
            return []

        assert len(_hooks) == 1

    def test_hook_preserves_function_callable(self) -> None:
        from osa.authoring.hook import hook

        @hook
        def detect(record: Record[SampleSchema]) -> list[PocketResult]:
            return []

        assert callable(detect)

    def test_hook_preserves_function_name(self) -> None:
        from osa.authoring.hook import hook

        @hook
        def detect_pockets(record: Record[SampleSchema]) -> list[PocketResult]:
            return []

        assert detect_pockets.__name__ == "detect_pockets"

    def test_multiple_hooks_register(self) -> None:
        from osa._registry import _hooks
        from osa.authoring.hook import hook

        @hook
        def detect(record: Record[SampleSchema]) -> list[PocketResult]:
            return []

        @hook
        def quality(record: Record[SampleSchema]) -> QualityResult:
            return QualityResult(atom_count=100, completeness=0.9)

        assert len(_hooks) == 2


class TestHookTypeIntrospection:
    def setup_method(self) -> None:
        from osa._registry import clear

        clear()

    def test_extracts_schema_type(self) -> None:
        from osa._registry import _hooks
        from osa.authoring.hook import hook

        @hook
        def detect(record: Record[SampleSchema]) -> list[PocketResult]:
            return []

        assert _hooks[0].schema_type is SampleSchema

    def test_extracts_output_type_from_list(self) -> None:
        from osa._registry import _hooks
        from osa.authoring.hook import hook

        @hook
        def detect(record: Record[SampleSchema]) -> list[PocketResult]:
            return []

        assert _hooks[0].output_type is PocketResult

    def test_extracts_output_type_from_scalar(self) -> None:
        from osa._registry import _hooks
        from osa.authoring.hook import hook

        @hook
        def check(record: Record[SampleSchema]) -> QualityResult:
            return QualityResult(atom_count=100, completeness=0.9)

        assert _hooks[0].output_type is QualityResult

    def test_extracts_name(self) -> None:
        from osa._registry import _hooks
        from osa.authoring.hook import hook

        @hook
        def detect_pockets(record: Record[SampleSchema]) -> list[PocketResult]:
            return []

        assert _hooks[0].name == "detect_pockets"


class TestHookCardinality:
    def setup_method(self) -> None:
        from osa._registry import clear

        clear()

    def test_list_return_is_many(self) -> None:
        from osa._registry import _hooks
        from osa.authoring.hook import hook

        @hook
        def detect(record: Record[SampleSchema]) -> list[PocketResult]:
            return []

        assert _hooks[0].cardinality == "many"

    def test_scalar_return_is_one(self) -> None:
        from osa._registry import _hooks
        from osa.authoring.hook import hook

        @hook
        def check(record: Record[SampleSchema]) -> QualityResult:
            return QualityResult(atom_count=100, completeness=0.9)

        assert _hooks[0].cardinality == "one"


class TestHookDependencies:
    def setup_method(self) -> None:
        from osa._registry import clear

        clear()

    def test_no_dependencies_for_simple_hook(self) -> None:
        from osa._registry import _hooks
        from osa.authoring.hook import hook

        @hook
        def detect(record: Record[SampleSchema]) -> list[PocketResult]:
            return []

        assert _hooks[0].dependencies == {}

    def test_extracts_dependencies(self) -> None:
        from osa._registry import _hooks
        from osa.authoring.hook import hook

        @hook
        def detect(
            record: Record[SampleSchema],
            quality: QualityResult,
        ) -> list[PocketResult]:
            return []

        assert "quality" in _hooks[0].dependencies
        assert _hooks[0].dependencies["quality"] is QualityResult


class TestHookRegistrySchemaType:
    """Verify @hook stores schema_type in the registry."""

    def setup_method(self) -> None:
        from osa._registry import clear

        clear()

    def test_schema_type_available_via_registry(self) -> None:
        from osa._registry import _hooks
        from osa.authoring.hook import hook

        @hook
        def detect(record: Record[SampleSchema]) -> list[PocketResult]:
            return []

        info = next(h for h in _hooks if h.fn is detect)
        assert info.schema_type is SampleSchema
