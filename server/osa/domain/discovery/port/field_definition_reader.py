"""FieldDefinitionReader port — cross-domain read port for schema field lookups."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from osa.domain.semantics.model.value import FieldType


class FieldDefinitionReader(Protocol):
    async def get_all_field_types(self) -> dict[str, FieldType]:
        """Return global field_name -> FieldType map across all schemas.

        Raises ValidationError if same field name has conflicting types across schemas.
        """
        ...
