"""MetadataStore port — DDL + DML for typed per-schema metadata tables."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from osa.domain.semantics.model.value import FieldDefinition
    from osa.domain.shared.model.srn import RecordSRN, SchemaId


class MetadataStore(Protocol):
    """Port owned by the metadata domain.

    Implementations are responsible for:
    - Creating the ``metadata.<schema_slug>_v<major>`` table on first
      registration for a ``(schema_id, major)`` pair.
    - Additively ALTER ADD COLUMN when the schema bumps (minor/patch) with
      new optional fields.
    - Appending version lineage into the catalog's ``schema_versions`` list.
    - Idempotent UPSERT of a row keyed on ``record_srn``.
    """

    async def ensure_table(
        self,
        schema_id: "SchemaId",
        fields: "list[FieldDefinition]",
    ) -> None:
        """Create or additively evolve the typed metadata table for a schema.

        The PG table slug is derived from ``schema_id.id.root`` — the schema's
        human-readable slug is the single source of truth for the storage name.
        """
        ...

    async def insert(
        self,
        schema_id: "SchemaId",
        record_srn: "RecordSRN",
        values: dict[str, Any],
    ) -> None:
        """Upsert a record's typed metadata row into the schema's table."""
        ...

    async def insert_many(
        self,
        schema_id: "SchemaId",
        rows: "list[tuple[RecordSRN, dict[str, Any]]]",
    ) -> None:
        """Bulk upsert typed metadata rows — one multi-row SQL statement.

        All rows must belong to the same schema; callers group by schema_id
        before calling. Empty ``rows`` is a no-op.
        """
        ...
