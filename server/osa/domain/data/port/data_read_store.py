"""Read-store ports feeding the ``/data/`` surface — split along the service seam.

``DataTableReadStore`` is the streaming primitive behind both the paginated
JSON path and the unbounded CSV/CSV.gz path; the concrete adapter backs it with
a Postgres server-side cursor (research §2) so memory stays bounded regardless
of result size. Rows are yielded as column→value mappings (already projected):
records-table rows include the implicit ``id``/``srn``/``schema_id``/
``version``/``created_at`` columns plus metadata fields; feature-table rows
carry the hook's declared columns.

``DataCatalogReadStore`` serves the non-streaming reads: node catalog, schema
manifest, latest-schema resolution, and single-record-by-id.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from osa.domain.data.model.catalog import NodeCatalog
    from osa.domain.data.model.manifest import SchemaManifest
    from osa.domain.data.model.query_plan import QueryPlan
    from osa.domain.data.model.record_summary import RecordSummary
    from osa.domain.shared.model.ids import RecordId
    from osa.domain.shared.model.srn import SchemaId


class DataTableReadStore(Protocol):
    def stream_rows(
        self, plan: "QueryPlan", timeout: timedelta | None = None
    ) -> AsyncIterator[Mapping[str, Any]]:
        """Stream projected rows for the plan via a server-side cursor.

        ``timeout`` is the caller's execution budget for the read; the adapter
        enforces it on its own connection (the routes hold no SQL session).
        """
        ...


class DataCatalogReadStore(Protocol):
    async def get_record_by_id(self, id: "RecordId", version: int | None) -> "RecordSummary | None":
        """Resolve a single record by bare ID (schema resolved via PK). ``None`` if absent."""
        ...

    async def get_node_catalog(self) -> "NodeCatalog":
        """List published schemas with summary table resources."""
        ...

    async def get_schema_manifest(self, schema_id: "SchemaId") -> "SchemaManifest | None":
        """Full manifest for a schema. ``None`` if unknown."""
        ...

    async def get_latest_schema_id(self, schema_short_id: str) -> "SchemaId | None":
        """Resolve a bare schema id to its latest published version. ``None`` if unknown."""
        ...
