"""Dependency injection provider for index backends."""
from osa.util.di.scope import Scope

from dishka import Provider, provide

from osa.config import Config
from osa.domain.index.model.registry import IndexRegistry
from osa.infrastructure.index.vector.backend import VectorStorageBackend
from osa.sdk.index.backend import StorageBackend


class IndexProvider(Provider):
    """Provides configured index backends."""

    @provide(scope=Scope.APP)
    def get_backends(self, config: Config) -> IndexRegistry:
        """Build all configured index backends.

        Returns:
            Registry of index backends.
        """
        backends: dict[str, StorageBackend] = {}

        for name, idx_config in config.indexes.items():
            if idx_config.backend == "vector":
                backends[name] = VectorStorageBackend(name, idx_config.config)
            # Add more backend types here as they're implemented
            # elif idx_config.backend == "keyword":
            #     backends[name] = KeywordStorageBackend(name, idx_config.config)

        return IndexRegistry(backends)
