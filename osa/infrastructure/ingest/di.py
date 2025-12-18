"""Dependency injection provider for ingestors."""

from dishka import Provider, Scope, provide

from osa.config import Config
from osa.infrastructure.ingest.geo.ingestor import GEOIngestor
from osa.sdk.ingest.ingestor import Ingestor


class IngestProvider(Provider):
    """Provides configured ingestors."""

    @provide(scope=Scope.APP)
    def get_ingestors(self, config: Config) -> dict[str, Ingestor]:
        """Build all configured ingestors.

        Returns:
            Dictionary mapping ingestor names to their instances.
        """
        ingestors: dict[str, Ingestor] = {}

        for name, ing_config in config.ingestors.items():
            match ing_config.ingestor:
                case "geo":
                    ingestors[name] = GEOIngestor(ing_config.config)
                case _:
                    raise ValueError(f"Unknown ingestor type: {ing_config.ingestor}")

        return ingestors
