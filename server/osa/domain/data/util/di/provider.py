"""Dishka DI provider for the data domain (services)."""

from dishka import provide

from osa.config import Config
from osa.domain.data.port.data_read_store import DataReadStore
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
