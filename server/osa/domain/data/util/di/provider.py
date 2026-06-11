"""Dishka DI provider for the data domain (services + query handlers)."""

from dishka import provide

from osa.config import Config
from osa.domain.data.port.data_read_store import DataReadStore
from osa.domain.data.query.catalog import (
    GetDataRecordHandler,
    GetNodeCatalogHandler,
    GetSchemaManifestHandler,
)
from osa.domain.data.query.read_table import (
    ReadFeatureTableHandler,
    ReadRecordsTableHandler,
)
from osa.domain.data.service.data_catalog import DataCatalogService
from osa.domain.data.service.data_query import DataQueryService
from osa.util.di.base import Provider
from osa.util.di.scope import Scope


class DataProvider(Provider):
    @provide(scope=Scope.UOW)
    def get_data_query_service(self, read_store: DataReadStore, config: Config) -> DataQueryService:
        return DataQueryService(read_store=read_store, config=config)

    @provide(scope=Scope.UOW)
    def get_data_catalog_service(self, read_store: DataReadStore) -> DataCatalogService:
        return DataCatalogService(read_store=read_store)

    read_records_table_handler = provide(ReadRecordsTableHandler, scope=Scope.UOW)
    read_feature_table_handler = provide(ReadFeatureTableHandler, scope=Scope.UOW)
    get_node_catalog_handler = provide(GetNodeCatalogHandler, scope=Scope.UOW)
    get_schema_manifest_handler = provide(GetSchemaManifestHandler, scope=Scope.UOW)
    get_data_record_handler = provide(GetDataRecordHandler, scope=Scope.UOW)
