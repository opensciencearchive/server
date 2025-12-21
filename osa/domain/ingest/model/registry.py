"""Ingestor registry - typed container for available ingestors."""

from collections.abc import Iterator

from osa.sdk.ingest.ingestor import Ingestor


class IngestorRegistry:
    """Registry of available ingestors."""

    def __init__(self, ingestors: dict[str, Ingestor]) -> None:
        self._ingestors = ingestors

    def get(self, name: str) -> Ingestor | None:
        """Get an ingestor by name."""
        return self._ingestors.get(name)

    def __contains__(self, name: str) -> bool:
        return name in self._ingestors

    def __iter__(self) -> Iterator[str]:
        return iter(self._ingestors)

    def __len__(self) -> int:
        return len(self._ingestors)

    def names(self) -> list[str]:
        """List all available ingestor names."""
        return list(self._ingestors.keys())
