"""Metadata schema definitions for OSA validators and transforms."""

from typing import Any

from pydantic import BaseModel, ConfigDict
from pydantic import Field as _PydanticField
from pydantic.fields import FieldInfo


class MetadataSchema(BaseModel):
    """Base class for defining typed metadata schemas.

    Subclass this to declare the metadata fields a record must provide.
    Uses Pydantic validation under the hood — fields support constraints
    like ``ge``, ``le``, ``pattern``, ``Literal``, etc.

    Extra fields not declared in the schema are rejected.
    """

    model_config = ConfigDict(extra="forbid")


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
