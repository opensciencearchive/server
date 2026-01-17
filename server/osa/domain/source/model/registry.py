"""Source registry - typed container for available sources."""

from collections.abc import Iterator

from osa.sdk.source.source import Source


class SourceRegistry:
    """Registry of available sources."""

    def __init__(self, sources: dict[str, Source]) -> None:
        self._sources = sources

    def get(self, name: str) -> Source | None:
        """Get a source by name."""
        return self._sources.get(name)

    def __contains__(self, name: str) -> bool:
        return name in self._sources

    def __iter__(self) -> Iterator[str]:
        return iter(self._sources)

    def __len__(self) -> int:
        return len(self._sources)

    def names(self) -> list[str]:
        """List all available source names."""
        return list(self._sources.keys())
