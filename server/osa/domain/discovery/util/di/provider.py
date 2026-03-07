"""Dishka DI provider for the discovery domain."""

from dishka import provide

from osa.domain.discovery.port.field_definition_reader import FieldDefinitionReader
from osa.domain.discovery.port.read_store import DiscoveryReadStore
from osa.domain.discovery.query.get_feature_catalog import GetFeatureCatalogHandler
from osa.domain.discovery.query.search_features import SearchFeaturesHandler
from osa.domain.discovery.query.search_records import SearchRecordsHandler
from osa.domain.discovery.service.discovery import DiscoveryService
from osa.util.di.base import Provider
from osa.util.di.scope import Scope


class DiscoveryProvider(Provider):
    @provide(scope=Scope.UOW)
    def get_discovery_service(
        self,
        read_store: DiscoveryReadStore,
        field_reader: FieldDefinitionReader,
    ) -> DiscoveryService:
        return DiscoveryService(read_store=read_store, field_reader=field_reader)

    # Query Handlers
    search_records_handler = provide(SearchRecordsHandler, scope=Scope.UOW)
    get_feature_catalog_handler = provide(GetFeatureCatalogHandler, scope=Scope.UOW)
    search_features_handler = provide(SearchFeaturesHandler, scope=Scope.UOW)
