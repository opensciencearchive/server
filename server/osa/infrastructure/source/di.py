"""Dependency injection provider for sources."""

from dishka import Provider, provide

from osa.config import Config
from osa.domain.shared.model.srn import Domain
from osa.domain.shared.outbox import Outbox
from osa.domain.source.model.registry import SourceRegistry
from osa.domain.source.service import SourceService
from osa.infrastructure.source.discovery import (
    discover_sources,
    validate_all_source_configs,
)
from osa.sdk.source.source import Source
from osa.util.di.scope import Scope


class SourceProvider(Provider):
    """Provides configured sources."""

    @provide(scope=Scope.APP)
    def get_sources(self, config: Config) -> SourceRegistry:
        """Build all configured sources.

        Discovers available sources via entry points, validates
        configuration, and instantiates each configured source.

        Returns:
            Registry of sources.
        """
        # Discover available source classes
        available = discover_sources()

        # Validate all configs and get (class, validated_config) pairs
        validated = validate_all_source_configs(config.sources, available)

        # Instantiate sources
        sources: dict[str, Source] = {}
        for name, (source_cls, validated_config) in validated.items():
            sources[name] = source_cls(validated_config)

        return SourceRegistry(sources)

    @provide(scope=Scope.UOW)
    def get_source_service(
        self,
        sources: SourceRegistry,
        outbox: Outbox,
        config: Config,
    ) -> SourceService:
        """Provide SourceService for UOW scope.

        SourceService is UOW-scoped because it needs fresh Outbox per unit of work.
        """
        return SourceService(
            sources=sources,
            outbox=outbox,
            node_domain=Domain(config.server.domain),
        )
