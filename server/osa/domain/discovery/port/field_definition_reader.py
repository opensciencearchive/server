"""FieldDefinitionReader port — cross-domain read port for schema field lookups."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from osa.domain.semantics.model.value import FieldType
    from osa.domain.shared.model.srn import SchemaId


class FieldDefinitionReader(Protocol):
    async def get_all_field_types(self) -> dict[str, FieldType]:
        """Return global field_name -> FieldType map across all schemas.

        Raises ValidationError if same field name has conflicting types across schemas.
        """
        ...

    async def get_fields_for_schema(self, schema_id: "SchemaId") -> dict[str, FieldType]:
        """Return field_name -> FieldType for a specific schema's current major version.

        Falls back to an empty dict when the schema is unknown to the node.
        """
        ...
