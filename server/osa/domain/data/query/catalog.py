"""Catalog-shaped query handlers — node catalog, schema manifest, record by id."""

from __future__ import annotations

from osa.domain.data.model.catalog import NodeCatalog
from osa.domain.data.model.manifest import SchemaManifest
from osa.domain.data.model.record_summary import RecordSummary
from osa.domain.data.service.data_catalog import DataCatalogService
from osa.domain.shared.authorization.gate import public
from osa.domain.shared.model.ids import RecordRef
from osa.domain.shared.query import Query, QueryHandler


class GetNodeCatalog(Query):
    pass


class GetNodeCatalogHandler(QueryHandler[GetNodeCatalog, NodeCatalog]):
    __auth__ = public()
    catalog_service: DataCatalogService

    async def run(self, cmd: GetNodeCatalog) -> NodeCatalog:
        return await self.catalog_service.get_node_catalog()


class GetSchemaManifest(Query):
    schema: str  # URL segment: ``<id>`` or ``<id>@<semver>``


class GetSchemaManifestHandler(QueryHandler[GetSchemaManifest, SchemaManifest]):
    __auth__ = public()
    catalog_service: DataCatalogService

    async def run(self, cmd: GetSchemaManifest) -> SchemaManifest:
        schema_id = await self.catalog_service.resolve_schema(cmd.schema)
        return await self.catalog_service.get_schema_manifest(schema_id)


class GetDataRecord(Query):
    ref: RecordRef


class GetDataRecordHandler(QueryHandler[GetDataRecord, RecordSummary]):
    __auth__ = public()
    catalog_service: DataCatalogService

    async def run(self, cmd: GetDataRecord) -> RecordSummary:
        return await self.catalog_service.get_record_by_id(cmd.ref.id, cmd.ref.version)
