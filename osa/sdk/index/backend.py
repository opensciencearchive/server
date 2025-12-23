"""StorageBackend protocol for pluggable index backends."""

from typing import Any, Protocol

from osa.sdk.index.result import QueryResult


class StorageBackend(Protocol):
    """Protocol for pluggable index storage backends.

    Implement this protocol to create custom storage backends
    (e.g., vector search, keyword search, graph databases).
    """

    @property
    def name(self) -> str:
        """Unique name for this index instance."""
        ...

    async def ingest(self, srn: str, record: dict[str, Any]) -> None:
        """Store a record in the index.

        Args:
            srn: Structured Resource Name identifying the record.
            record: The record metadata to index.
        """
        ...

    async def delete(self, srn: str) -> None:
        """Remove a record from the index.

        Args:
            srn: Structured Resource Name of the record to remove.
        """
        ...

    async def query(self, q: str, limit: int = 20) -> QueryResult:
        """Execute a query and return structured results.

        Args:
            q: The query string.
            limit: Maximum number of results to return.

        Returns:
            QueryResult containing matching hits.
        """
        ...

    async def health(self) -> bool:
        """Check if the backend is operational.

        Returns:
            True if the backend is healthy, False otherwise.
        """
        ...

    async def count(self) -> int:
        """Return the number of documents in the index.

        Returns:
            Number of indexed documents.
        """
        ...
