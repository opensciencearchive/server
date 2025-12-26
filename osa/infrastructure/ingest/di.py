"""Dependency injection provider for ingestors."""

from dishka import Provider, provide

from osa.config import Config
from osa.domain.ingest.model.registry import IngestorRegistry
from osa.infrastructure.ingest.discovery import (
    discover_ingestors,
    validate_all_ingestor_configs,
)
from osa.sdk.ingest.ingestor import Ingestor
from osa.util.di.scope import Scope


class IngestProvider(Provider):
    """Provides configured ingestors."""

    @provide(scope=Scope.APP)
    def get_ingestors(self, config: Config) -> IngestorRegistry:
        """Build all configured ingestors.

        Discovers available ingestors via entry points, validates
        configuration, and instantiates each configured ingestor.

        Returns:
            Registry of ingestors.
        """
        # Discover available ingestor classes
        available = discover_ingestors()

        # Validate all configs and get (class, validated_config) pairs
        validated = validate_all_ingestor_configs(config.ingestors, available)

        # Instantiate ingestors
        ingestors: dict[str, Ingestor] = {}
        for name, (ingestor_cls, validated_config) in validated.items():
            ingestors[name] = ingestor_cls(validated_config)

        return IngestorRegistry(ingestors)
