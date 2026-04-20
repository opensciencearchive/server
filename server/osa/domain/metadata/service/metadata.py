"""MetadataService — thin delegator over the MetadataStore port."""

from __future__ import annotations

from typing import Any

from osa.domain.metadata.port.metadata_store import MetadataStore
from osa.domain.semantics.model.value import FieldDefinition
from osa.domain.shared.model.srn import RecordSRN, SchemaSRN
from osa.domain.shared.service import Service


class MetadataService(Service):
    """Creates/evolves typed metadata tables and inserts record metadata."""

    metadata_store: MetadataStore

    async def ensure_table(
        self,
        schema_srn: SchemaSRN,
        schema_title: str,
        fields: list[FieldDefinition],
    ) -> None:
        await self.metadata_store.ensure_table(schema_srn, schema_title, fields)

    async def insert(
        self,
        schema_srn: SchemaSRN,
        record_srn: RecordSRN,
        values: dict[str, Any],
    ) -> None:
        await self.metadata_store.insert(schema_srn, record_srn, values)
