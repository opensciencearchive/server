"""Source protocol for pluggable data sources."""

from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any, ClassVar, Protocol

from pydantic import BaseModel

from osa.sdk.source.record import UpstreamRecord

# Type alias for pull() return: (records_iterator, session_for_next_chunk)
PullResult = tuple[AsyncIterator[UpstreamRecord], dict[str, Any] | None]


class Source(Protocol):
    """Protocol for pluggable data sources.

    Implement this protocol to create custom sources
    for different data origins (e.g., GEO, ENA, Zenodo, user uploads).

    Class attributes:
        name: Unique identifier for this source (e.g., 'geo-entrez').
            Must match the entry point name.
        config_class: Pydantic model for validating configuration.
    """

    name: ClassVar[str]
    config_class: ClassVar[type[BaseModel]]

    def __init__(self, config: BaseModel) -> None:
        """Initialize the source with validated configuration."""
        ...

    async def pull(
        self,
        since: datetime | None = None,
        limit: int | None = None,
        offset: int = 0,
        session: dict[str, Any] | None = None,
    ) -> PullResult:
        """Pull records from an upstream source.

        This method returns an async iterator that yields records, plus
        session state to pass to the next chunk for efficient pagination.

        Args:
            since: Only fetch records updated after this time.
            limit: Maximum number of records to fetch.
            offset: Skip first N records (for chunked processing).
            session: Opaque pagination state from previous chunk (e.g., NCBI WebEnv).

        Returns:
            Tuple of (records_iterator, session_for_next_chunk).
            The session is opaque state to pass to the next chunk.
            Returns None for session if no more chunks are needed or
            if the source doesn't support session-based pagination.
        """
        ...

    async def get_one(self, source_id: str) -> UpstreamRecord | None:
        """Fetch a single record by its source-specific ID.

        Args:
            source_id: The source-specific identifier (e.g., "GSE12345").

        Returns:
            UpstreamRecord if found, None otherwise.
        """
        ...

    async def health(self) -> bool:
        """Check if the upstream source is reachable.

        Returns:
            True if the source is healthy, False otherwise.
        """
        ...
