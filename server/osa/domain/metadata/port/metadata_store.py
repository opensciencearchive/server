"""MetadataStore port — DDL + DML for typed per-schema metadata tables."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from osa.domain.semantics.model.value import FieldDefinition
    from osa.domain.shared.model.srn import RecordSRN, SchemaSRN


class MetadataStore(Protocol):
    """Port owned by the metadata domain.

    Implementations are responsible for:
    - Creating the ``metadata.<schema_slug>_v<major>`` table on first
      registration for a (schema_identity, major) pair.
    - Additively ALTER ADD COLUMN when the schema bumps (minor/patch) with
      new optional fields.
    - Appending SRN lineage into the catalog's ``schema_versions`` list.
    - Idempotent UPSERT of a row keyed on ``record_srn``.
    """

    async def ensure_table(
        self,
        schema_srn: "SchemaSRN",
        schema_title: str,
        fields: "list[FieldDefinition]",
    ) -> None:
        """Create or additively evolve the typed metadata table for a schema."""
        ...

    async def insert(
        self,
        schema_srn: "SchemaSRN",
        record_srn: "RecordSRN",
        values: dict[str, Any],
    ) -> None:
        """Upsert a record's typed metadata row into the schema's table."""
        ...
