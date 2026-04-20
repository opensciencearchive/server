"""Typed field references used inside Predicate.field.

Two kinds of references are supported:

- :class:`MetadataFieldRef` — resolves to a column in ``metadata.<schema_slug>_v<major>``.
- :class:`FeatureFieldRef` — resolves to a column in ``features.<hook>``.

Wire format is a dotted path (``metadata.<field>`` or
``features.<hook>.<column>``). :func:`parse_field_ref` parses the wire form
into a typed reference and validates identifier shape.
"""

from __future__ import annotations

import re
from typing import Literal, Union

from pydantic import BaseModel

_IDENT = re.compile(r"^[a-z][a-z0-9_]*$")


class MetadataFieldRef(BaseModel):
    path: Literal["metadata"] = "metadata"
    field: str

    def dotted(self) -> str:
        return f"metadata.{self.field}"


class FeatureFieldRef(BaseModel):
    path: Literal["features"] = "features"
    hook: str
    column: str

    def dotted(self) -> str:
        return f"features.{self.hook}.{self.column}"


FieldRef = Union[MetadataFieldRef, FeatureFieldRef]


def parse_field_ref(dotted: str) -> FieldRef:
    """Parse a dotted-path field reference into its typed form.

    Raises :class:`ValueError` when the path shape or identifier doesn't match
    the documented grammar.
    """
    if not isinstance(dotted, str):
        raise ValueError(f"Expected dotted string, got {type(dotted).__name__}")

    parts = dotted.split(".")
    if not parts:
        raise ValueError(f"Empty field reference: {dotted!r}")

    head = parts[0]
    if head == "metadata":
        if len(parts) != 2:
            raise ValueError(f"metadata.* refs must be exactly two dotted parts, got {dotted!r}")
        field = parts[1]
        if not _IDENT.match(field):
            raise ValueError(f"Invalid metadata field identifier: {field!r}")
        return MetadataFieldRef(field=field)

    if head == "features":
        if len(parts) != 3:
            raise ValueError(f"features.* refs must be exactly three dotted parts, got {dotted!r}")
        hook, column = parts[1], parts[2]
        if not _IDENT.match(hook):
            raise ValueError(f"Invalid hook identifier: {hook!r}")
        if not _IDENT.match(column):
            raise ValueError(f"Invalid feature column identifier: {column!r}")
        return FeatureFieldRef(hook=hook, column=column)

    raise ValueError(
        f"Unknown field reference prefix {head!r} in {dotted!r}. "
        "Expected 'metadata.<field>' or 'features.<hook>.<column>'."
    )
