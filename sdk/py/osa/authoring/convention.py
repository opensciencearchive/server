"""Convention registration function."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from osa._registry import ConventionInfo, _conventions
from osa.types.schema import MetadataSchema


def convention(
    *,
    title: str,
    schema: type[MetadataSchema],
    files: dict[str, Any],
    hooks: list[Callable],
) -> None:
    """Register a convention that composes schemas and hooks."""
    _conventions.append(
        ConventionInfo(
            title=title,
            schema_type=schema,
            file_requirements=files,
            hooks=hooks,
        )
    )
