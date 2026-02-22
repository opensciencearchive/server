"""Metadata schema definitions for OSA validators and transforms."""

from __future__ import annotations

import typing
from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict
from pydantic import Field as _PydanticField
from pydantic.fields import FieldInfo

# Python type → (FieldType, extra_constraints)
_TYPE_MAP: dict[type, str] = {
    str: "text",
    int: "number",
    float: "number",
    bool: "boolean",
    date: "date",
    datetime: "date",
}


class MetadataSchema(BaseModel):
    """Base class for defining typed metadata schemas.

    Subclass this to declare the metadata fields a record must provide.
    Uses Pydantic validation under the hood — fields support constraints
    like ``ge``, ``le``, ``pattern``, ``Literal``, etc.

    Extra fields not declared in the schema are rejected.
    """

    model_config = ConfigDict(extra="forbid")

    @classmethod
    def to_field_definitions(cls) -> list[dict[str, Any]]:
        """Convert this schema's fields to server FieldDefinition dicts.

        Maps Python type hints to the server's FieldType format:
            str → text, int → number (integer_only), float → number,
            bool → boolean, date/datetime → date, T | None → required=False.
        """
        result: list[dict[str, Any]] = []
        for name, field_info in cls.model_fields.items():
            annotation = field_info.annotation
            required = field_info.is_required()

            # Unwrap Optional[T] / T | None
            inner = _unwrap_optional(annotation)

            # Resolve field type
            field_type = _TYPE_MAP.get(inner, "text")

            field_def: dict[str, Any] = {
                "name": name,
                "type": field_type,
                "required": required,
                "cardinality": "exactly_one",
            }

            # Build constraints (discriminated union with "type" key)
            constraints: dict[str, Any] | None = None
            if field_type == "number":
                c: dict[str, Any] = {"type": "number"}
                if inner is int:
                    c["integer_only"] = True
                extra = field_info.json_schema_extra
                if isinstance(extra, dict) and "unit" in extra:
                    c["unit"] = extra["unit"]
                constraints = c
            elif field_type == "text":
                extra = field_info.json_schema_extra
                if isinstance(extra, dict):
                    constraints = {"type": "text", **extra}

            if constraints:
                field_def["constraints"] = constraints

            result.append(field_def)
        return result


def _unwrap_optional(annotation: Any) -> type:
    """Unwrap Optional[T] / T | None to get the inner type."""
    origin = typing.get_origin(annotation)
    args = typing.get_args(annotation)
    if origin is typing.Union:
        non_none = [a for a in args if a is not type(None)]
        if non_none:
            return non_none[0]
    return annotation


def Field(*, unit: str | None = None, **kwargs: Any) -> FieldInfo:
    """Declare a metadata field with optional unit annotation.

    Thin wrapper around :func:`pydantic.Field` that adds an optional
    ``unit`` keyword. The unit value is stored in ``json_schema_extra``
    and appears in the generated JSON Schema.
    """
    extra: dict[str, Any] = kwargs.pop("json_schema_extra", None) or {}
    if unit is not None:
        extra["unit"] = unit
    if extra:
        kwargs["json_schema_extra"] = extra
    return _PydanticField(**kwargs)
