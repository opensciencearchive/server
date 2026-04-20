"""Metadata domain value objects — MetadataSchema, slug helpers."""

from __future__ import annotations

from osa.domain.shared.model.hook import ColumnDef
from osa.domain.shared.model.value import ValueObject


class MetadataSchema(ValueObject):
    """Typed projection of a Schema into dynamic-column form.

    Mirrors :class:`FeatureSchema` — serialised into the catalog row's
    ``metadata_schema`` JSONB column and rehydrated on subsequent reads.
    """

    columns: list[ColumnDef] = []
