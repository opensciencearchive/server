"""Dependency injection provider for index backends."""

from dishka import Provider, provide

from osa.domain.index.model.registry import IndexRegistry
from osa.domain.index.service import IndexService
from osa.util.di.scope import Scope


class IndexProvider(Provider):
    """Provides configured index backends."""

    @provide(scope=Scope.APP)
    def get_backends(self) -> IndexRegistry:
        """Build empty index registry (indexes are dormant)."""
        return IndexRegistry({})

    @provide(scope=Scope.UOW)
    def get_index_service(self, indexes: IndexRegistry) -> IndexService:
        """Provide IndexService for UOW scope."""
        return IndexService(indexes=indexes)
