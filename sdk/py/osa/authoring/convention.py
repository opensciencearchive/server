"""Convention registration function."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from osa._registry import ConventionInfo, _conventions, register_source
from osa.types.schema import MetadataSchema


def convention(
    *,
    title: str,
    version: str = "0.0.0",
    schema: type[MetadataSchema],
    source: type | None = None,
    files: dict[str, Any],
    hooks: list[Callable],
) -> None:
    """Register a convention that composes schemas, hooks, and an optional source."""
    source_info = None
    if source is not None:
        source_info = register_source(source)

    _conventions.append(
        ConventionInfo(
            title=title,
            version=version,
            schema_type=schema,
            file_requirements=files,
            hooks=hooks,
            source_type=source,
            source_info=source_info,
        )
    )
