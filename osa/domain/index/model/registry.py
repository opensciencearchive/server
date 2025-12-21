"""Index registry - typed container for available storage backends."""

from collections.abc import Iterator

from osa.sdk.index.backend import StorageBackend


class IndexRegistry:
    """Registry of available index backends."""

    def __init__(self, backends: dict[str, StorageBackend]) -> None:
        self._backends = backends

    def get(self, name: str) -> StorageBackend | None:
        """Get a backend by name."""
        return self._backends.get(name)

    def __contains__(self, name: str) -> bool:
        return name in self._backends

    def __iter__(self) -> Iterator[str]:
        return iter(self._backends)

    def __len__(self) -> int:
        return len(self._backends)

    def names(self) -> list[str]:
        """List all available index names."""
        return list(self._backends.keys())

    def items(self) -> Iterator[tuple[str, StorageBackend]]:
        """Iterate over (name, backend) pairs."""
        return iter(self._backends.items())
