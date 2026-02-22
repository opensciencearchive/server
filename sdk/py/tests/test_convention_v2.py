"""Tests for updated convention() with version and source support."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any

from pydantic import BaseModel

from osa.types.record import Record
from osa.types.schema import MetadataSchema


class SampleSchema(MetadataSchema):
    organism: str


class PocketResult(BaseModel):
    pocket_id: str
    score: float


class TestConventionVersion:
    def setup_method(self) -> None:
        from osa._registry import clear

        clear()

    def test_convention_stores_version(self) -> None:
        from osa._registry import _conventions
        from osa.authoring.convention import convention
        from osa.authoring.hook import hook

        @hook
        def detect(record: Record[SampleSchema]) -> list[PocketResult]:
            return []

        convention(
            title="Test Convention",
            version="1.0.0",
            schema=SampleSchema,
            files={"accepted_types": [".cif"]},
            hooks=[detect],
        )
        assert _conventions[0].version == "1.0.0"

    def test_convention_stores_source_type(self) -> None:
        from osa._registry import _conventions
        from osa.authoring.convention import convention
        from osa.authoring.hook import hook
        from osa.authoring.source import Source
        from osa.runtime.source_context import SourceContext
        from osa.types.source import SourceRecord

        @hook
        def detect(record: Record[SampleSchema]) -> list[PocketResult]:
            return []

        class MySource(Source):
            name = "test-source"

            class RuntimeConfig(BaseModel):
                api_key: str

            async def pull(
                self,
                *,
                ctx: SourceContext,
                since: datetime | None = None,
                limit: int | None = None,
                offset: int = 0,
                session: dict[str, Any] | None = None,
            ) -> AsyncIterator[SourceRecord]:
                yield  # type: ignore[misc]  # pragma: no cover

        convention(
            title="Test Convention",
            version="1.0.0",
            schema=SampleSchema,
            source=MySource,
            files={"accepted_types": [".cif"]},
            hooks=[detect],
        )
        assert _conventions[0].source_type is MySource

    def test_convention_populates_source_info(self) -> None:
        from osa._registry import _conventions
        from osa.authoring.convention import convention
        from osa.authoring.hook import hook
        from osa.authoring.source import Source
        from osa.runtime.source_context import SourceContext
        from osa.types.source import SourceRecord

        @hook
        def detect(record: Record[SampleSchema]) -> list[PocketResult]:
            return []

        class MySource(Source):
            name = "test-source"

            class RuntimeConfig(BaseModel):
                api_key: str

            async def pull(
                self,
                *,
                ctx: SourceContext,
                since: datetime | None = None,
                limit: int | None = None,
                offset: int = 0,
                session: dict[str, Any] | None = None,
            ) -> AsyncIterator[SourceRecord]:
                yield  # type: ignore[misc]  # pragma: no cover

        convention(
            title="Test Convention",
            version="1.0.0",
            schema=SampleSchema,
            source=MySource,
            files={"accepted_types": [".cif"]},
            hooks=[detect],
        )
        assert _conventions[0].source_info is not None
        assert _conventions[0].source_info.name == "test-source"
        assert _conventions[0].source_info.source_cls is MySource

    def test_convention_source_defaults_to_none(self) -> None:
        from osa._registry import _conventions
        from osa.authoring.convention import convention
        from osa.authoring.hook import hook

        @hook
        def detect(record: Record[SampleSchema]) -> list[PocketResult]:
            return []

        convention(
            title="No Source",
            version="1.0.0",
            schema=SampleSchema,
            files={},
            hooks=[detect],
        )
        assert _conventions[0].source_type is None
        assert _conventions[0].source_info is None

    def test_backward_compatible_without_version(self) -> None:
        """version defaults to '0.0.0' if omitted."""
        from osa._registry import _conventions
        from osa.authoring.convention import convention
        from osa.authoring.hook import hook

        @hook
        def detect(record: Record[SampleSchema]) -> list[PocketResult]:
            return []

        convention(
            title="No Version",
            schema=SampleSchema,
            files={},
            hooks=[detect],
        )
        assert _conventions[0].version == "0.0.0"


class TestManifestWithVersion:
    def setup_method(self) -> None:
        from osa._registry import clear

        clear()

    def test_manifest_convention_has_version(self) -> None:
        from osa.authoring.convention import convention
        from osa.authoring.hook import hook
        from osa.manifest import generate_manifest

        @hook
        def detect(record: Record[SampleSchema]) -> list[PocketResult]:
            return []

        convention(
            title="Test",
            version="2.1.0",
            schema=SampleSchema,
            files={},
            hooks=[detect],
        )
        m = generate_manifest()
        assert m.conventions[0].version == "2.1.0"

    def test_manifest_convention_has_source_name(self) -> None:
        from osa.authoring.convention import convention
        from osa.authoring.hook import hook
        from osa.authoring.source import Source
        from osa.manifest import generate_manifest
        from osa.runtime.source_context import SourceContext
        from osa.types.source import SourceRecord

        @hook
        def detect(record: Record[SampleSchema]) -> list[PocketResult]:
            return []

        class MySource(Source):
            name = "my-source"

            class RuntimeConfig(BaseModel):
                pass

            async def pull(
                self,
                *,
                ctx: SourceContext,
                since: datetime | None = None,
                limit: int | None = None,
                offset: int = 0,
                session: dict[str, Any] | None = None,
            ) -> AsyncIterator[SourceRecord]:
                yield  # type: ignore[misc]  # pragma: no cover

        convention(
            title="Test",
            version="1.0.0",
            schema=SampleSchema,
            source=MySource,
            files={},
            hooks=[detect],
        )
        m = generate_manifest()
        assert m.conventions[0].source_name == "my-source"

    def test_manifest_convention_source_name_none_when_no_source(self) -> None:
        from osa.authoring.convention import convention
        from osa.authoring.hook import hook
        from osa.manifest import generate_manifest

        @hook
        def detect(record: Record[SampleSchema]) -> list[PocketResult]:
            return []

        convention(
            title="Test",
            version="1.0.0",
            schema=SampleSchema,
            files={},
            hooks=[detect],
        )
        m = generate_manifest()
        assert m.conventions[0].source_name is None
