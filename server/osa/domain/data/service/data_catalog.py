"""DataCatalogService — catalog, manifest, and single-record-by-ID reads.

Read-only orchestration over the :class:`DataReadStore` port. Catalog + manifest
(US3) and record-by-ID (US4). The reserved-name 404 is enforced defensively
here in addition to the write-side aggregate invariants (research §6).
"""

from __future__ import annotations

from osa.domain.data.model.catalog import NodeCatalog
from osa.domain.data.model.manifest import ResolvedTable, SchemaManifest
from osa.domain.data.model.query_plan import TableKind
from osa.domain.data.model.record_summary import RecordSummary
from osa.domain.data.port.data_read_store import DataReadStore
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.model.ids import HookName, RecordId
from osa.domain.shared.model.reserved import RESERVED_NAMES
from osa.domain.shared.model.srn import SchemaId
from osa.domain.shared.service import Service


class DataCatalogService(Service):
    read_store: DataReadStore

    async def resolve_schema(self, raw: str) -> SchemaId:
        """Resolve a URL schema segment (``<id>`` or ``<id>@<semver>``) to a SchemaId.

        A bare id resolves to the latest published version. Reserved names and
        unknown schemas raise ``NotFoundError`` (404 at the route).
        """
        short_id = raw.split("@", 1)[0]
        if short_id in RESERVED_NAMES:
            raise NotFoundError(
                f"The path '{short_id}' is reserved under /data/.", code="reserved_name"
            )
        if "@" in raw:
            try:
                return SchemaId.parse(raw)
            except ValueError as exc:
                raise NotFoundError(
                    f"No schema '{raw}'. See /api/v1/data for the catalog.",
                    code="schema_not_found",
                ) from exc
        resolved = await self.read_store.get_latest_schema_id(short_id)
        if resolved is None:
            raise NotFoundError(
                f"No schema '{raw}'. See /api/v1/data for the catalog.",
                code="schema_not_found",
            )
        return resolved

    async def get_node_catalog(self) -> NodeCatalog:
        return await self.read_store.get_node_catalog()

    async def get_schema_manifest(self, schema_id: SchemaId) -> SchemaManifest:
        # Defense in depth: a reserved id can't be registered (write-side
        # invariant) but the read side handles the URL slot deliberately.
        if schema_id.id.root in RESERVED_NAMES:
            raise NotFoundError(
                f"The path '{schema_id.id.root}' is reserved under /data/.",
                code="reserved_name",
            )
        manifest = await self.read_store.get_schema_manifest(schema_id)
        if manifest is None:
            raise NotFoundError(
                f"No schema '{schema_id.render()}'. See /api/v1/data for the catalog.",
                code="schema_not_found",
            )
        return manifest

    async def resolve_table(
        self,
        schema: str,
        table_kind: TableKind,
        feature_name: HookName | None = None,
    ) -> ResolvedTable:
        """Resolve a URL schema segment + table selector to its column schema.

        Owns the manifest-structure facts the routes must not re-derive: the
        records resource is always named ``records``; a feature resource is
        matched by name AND kind (a hook cannot shadow the records slot).
        Unknown schema or table raises ``NotFoundError`` (404 before bytes).
        """
        schema_id = await self.resolve_schema(schema)
        manifest = await self.get_schema_manifest(schema_id)
        name = "records" if table_kind == TableKind.RECORDS else feature_name
        resource = next(
            (tr for tr in manifest.table_resources if tr.name == name and tr.kind == table_kind),
            None,
        )
        if resource is None:
            raise NotFoundError(
                f"No table '{name}' on schema '{schema}'. "
                f"See /api/v1/data/{schema_id.render()} for its table resources.",
                code="table_not_found",
            )
        return ResolvedTable(schema_id=schema_id, columns=resource.columns)

    async def get_record_by_id(self, id: RecordId, version: int | None) -> RecordSummary:
        record = await self.read_store.get_record_by_id(id, version)
        if record is None:
            suffix = f"@{version}" if version is not None else ""
            raise NotFoundError(f"No record with id '{id}{suffix}'.", code="record_not_found")
        return record
