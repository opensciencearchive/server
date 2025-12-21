"""Dependency injection provider for ingestors."""
from osa.util.di.scope import Scope

from dishka import Provider, provide

from osa.config import Config
from osa.domain.ingest.model.registry import IngestorRegistry
from osa.infrastructure.ingest.geo.ingestor import GEOIngestor
from osa.sdk.ingest.ingestor import Ingestor


class IngestProvider(Provider):
    """Provides configured ingestors."""

    @provide(scope=Scope.APP)
    def get_ingestors(self, config: Config) -> IngestorRegistry:
        """Build all configured ingestors.

        Returns:
            Registry of ingestors.
        """
        ingestors: dict[str, Ingestor] = {}

        for name, ing_config in config.ingestors.items():
            match ing_config.ingestor:
                case "geo":
                    ingestors[name] = GEOIngestor(ing_config.config)
                case _:
                    raise ValueError(f"Unknown ingestor type: {ing_config.ingestor}")

        return IngestorRegistry(ingestors)
