"""Dishka DI provider for the data domain (services + query handlers)."""

from dishka import provide

from osa.config import Config
from osa.domain.data.port.data_read_store import (
    DataCatalogReadStore,
    DataTableReadStore,
)
from osa.domain.data.query.catalog import (
    GetDataRecordHandler,
    GetNodeCatalogHandler,
    GetSchemaManifestHandler,
)
from osa.domain.data.query.read_table import (
    ReadFeatureTableHandler,
    ReadRecordsTableHandler,
)
from osa.domain.data.query.skill import (
    GetRootDiscoveryHandler,
    GetSchemaReferenceHandler,
    GetSkillDocumentHandler,
)
from osa.domain.data.service.data_catalog import DataCatalogService
from osa.domain.data.service.data_query import DataQueryService
from osa.domain.data.service.skill_generator import SkillGeneratorService
from osa.domain.data.service.skill_renderer import SkillRenderer
from osa.util.di.base import Provider
from osa.util.di.scope import Scope


class DataProvider(Provider):
    @provide(scope=Scope.UOW)
    def get_data_query_service(
        self, read_store: DataTableReadStore, config: Config
    ) -> DataQueryService:
        return DataQueryService(read_store=read_store, config=config)

    @provide(scope=Scope.UOW)
    def get_data_catalog_service(self, read_store: DataCatalogReadStore) -> DataCatalogService:
        return DataCatalogService(read_store=read_store)

    @provide(scope=Scope.APP)
    def get_skill_renderer(self) -> SkillRenderer:
        return SkillRenderer()

    @provide(scope=Scope.UOW)
    def get_skill_generator_service(
        self,
        catalog_service: DataCatalogService,
        read_store: DataCatalogReadStore,
        renderer: SkillRenderer,
        config: Config,
    ) -> SkillGeneratorService:
        return SkillGeneratorService(
            catalog_service=catalog_service,
            read_store=read_store,
            renderer=renderer,
            config=config,
        )

    read_records_table_handler = provide(ReadRecordsTableHandler, scope=Scope.UOW)
    read_feature_table_handler = provide(ReadFeatureTableHandler, scope=Scope.UOW)
    get_node_catalog_handler = provide(GetNodeCatalogHandler, scope=Scope.UOW)
    get_schema_manifest_handler = provide(GetSchemaManifestHandler, scope=Scope.UOW)
    get_data_record_handler = provide(GetDataRecordHandler, scope=Scope.UOW)
    get_root_discovery_handler = provide(GetRootDiscoveryHandler, scope=Scope.UOW)
    get_skill_document_handler = provide(GetSkillDocumentHandler, scope=Scope.UOW)
    get_schema_reference_handler = provide(GetSchemaReferenceHandler, scope=Scope.UOW)
