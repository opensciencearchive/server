"""Dependency injection provider for index backends."""

from dishka import Provider, provide

from osa.cli.util.paths import OSAPaths
from osa.config import Config
from osa.domain.index.model.registry import IndexRegistry
from osa.domain.index.service import IndexService
from osa.infrastructure.index.vector.backend import VectorStorageBackend
from osa.infrastructure.index.vector.config import VectorBackendConfig
from osa.sdk.index.backend import StorageBackend
from osa.util.di.scope import Scope


class IndexProvider(Provider):
    """Provides configured index backends."""

    @provide(scope=Scope.APP)
    def get_backends(self, config: Config, paths: OSAPaths) -> IndexRegistry:
        """Build all configured index backends.

        Args:
            config: Application configuration with index configs.
            paths: OSAPaths for deriving default persist_dir.

        Returns:
            Registry of index backends.
        """
        backends: dict[str, StorageBackend] = {}

        for idx_config in config.indexes:
            if idx_config.backend == "vector":
                vector_config = idx_config.config
                # Derive persist_dir from OSAPaths if not explicitly set
                if (
                    isinstance(vector_config, VectorBackendConfig)
                    and vector_config.persist_dir is None
                ):
                    vector_config = VectorBackendConfig(
                        persist_dir=paths.vectors_dir,
                        embedding=vector_config.embedding,
                    )
                backends[idx_config.name] = VectorStorageBackend(idx_config.name, vector_config)
            # Add more backend types here as they're implemented
            # elif idx_config.backend == "keyword":
            #     backends[idx_config.name] = KeywordStorageBackend(
            #         idx_config.name, idx_config.config
            #     )

        return IndexRegistry(backends)

    @provide(scope=Scope.UOW)
    def get_index_service(self, indexes: IndexRegistry) -> IndexService:
        """Provide IndexService for UOW scope.

        IndexService is UOW-scoped for consistency with other services,
        though it doesn't require Outbox (no events emitted).
        """
        return IndexService(indexes=indexes)
