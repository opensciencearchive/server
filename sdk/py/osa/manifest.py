"""Manifest generation for OSA deployments.

Introspects the hook registry to produce a typed, serializable
manifest describing all hooks and conventions in a project.
"""

from __future__ import annotations

import typing
from datetime import date, datetime
from typing import Any, get_args, get_origin
from uuid import UUID

from pydantic import BaseModel

from osa._registry import HookInfo, _hooks


class ColumnDef(BaseModel):
    """Definition of a single column in a feature table."""

    name: str
    json_type: str
    format: str | None = None
    required: bool


class FeatureSchema(BaseModel):
    """Typed column definitions for features a hook produces."""

    columns: list[ColumnDef]


class HookManifest(BaseModel):
    """Manifest entry for a single hook."""

    name: str
    record_schema: str
    cardinality: str
    feature_schema: FeatureSchema
    runner: str = "oci"


class ConventionManifest(BaseModel):
    """Manifest entry for a convention."""

    title: str
    record_schema: str
    file_requirements: dict[str, Any]
    hook_names: list[str]


class Manifest(BaseModel):
    """Full deployment manifest."""

    hooks: list[HookManifest]
    conventions: list[ConventionManifest] = []
    schemas: dict[str, dict]


# ---- Type mapping for FeatureSchema generation ----

_PYTHON_TYPE_TO_JSON: dict[type, tuple[str, str | None]] = {
    str: ("string", None),
    float: ("number", None),
    int: ("integer", None),
    bool: ("boolean", None),
    datetime: ("string", "date-time"),
    date: ("string", "date"),
    UUID: ("string", "uuid"),
}


def _resolve_json_type(annotation: Any) -> tuple[str, str | None]:
    """Map a Python type annotation to (json_type, format)."""
    # Unwrap Optional[T] / T | None
    origin = get_origin(annotation)
    args = get_args(annotation)

    if origin is typing.Union:
        non_none = [a for a in args if a is not type(None)]
        if non_none:
            return _resolve_json_type(non_none[0])

    # Handle list[X] → array
    if origin is list:
        return ("array", None)

    # Handle dict[X, Y] → object
    if origin is dict:
        return ("object", None)

    # Direct type lookup
    if annotation in _PYTHON_TYPE_TO_JSON:
        return _PYTHON_TYPE_TO_JSON[annotation]

    return ("string", None)


def _is_required(field_info: Any) -> bool:
    """Determine if a Pydantic field is required (non-optional)."""
    return field_info.is_required()


def generate_feature_schema(model_cls: type[BaseModel]) -> FeatureSchema:
    """Generate a FeatureSchema from a Pydantic BaseModel."""
    columns: list[ColumnDef] = []
    for name, field_info in model_cls.model_fields.items():
        json_type, fmt = _resolve_json_type(field_info.annotation)
        columns.append(
            ColumnDef(
                name=name,
                json_type=json_type,
                format=fmt,
                required=_is_required(field_info),
            )
        )
    return FeatureSchema(columns=columns)


# ---- Manifest generation ----


def _json_schema(cls: type) -> dict:
    """Extract JSON Schema from a Pydantic model."""
    if hasattr(cls, "model_json_schema"):
        return cls.model_json_schema()
    return {}


def _build_hook(info: HookInfo) -> HookManifest:
    """Build a HookManifest from introspected HookInfo."""
    feature_schema = FeatureSchema(columns=[])
    if info.output_type is not None and hasattr(info.output_type, "model_fields"):
        feature_schema = generate_feature_schema(info.output_type)

    return HookManifest(
        name=info.name,
        record_schema=info.schema_type.__name__,
        cardinality=info.cardinality,
        feature_schema=feature_schema,
        runner="oci",
    )


def generate_manifest() -> Manifest:
    """Generate the full deployment manifest from all registered hooks."""
    from osa._registry import _conventions

    hooks = [_build_hook(info) for info in _hooks]

    conventions = [
        ConventionManifest(
            title=c.title,
            record_schema=c.schema_type.__name__,
            file_requirements=c.file_requirements,
            hook_names=[h.__name__ for h in c.hooks],
        )
        for c in _conventions
    ]

    return Manifest(
        hooks=hooks,
        conventions=conventions,
        schemas={
            info.schema_type.__name__: _json_schema(info.schema_type)
            for info in _hooks
            if info.schema_type is not None
        },
    )
