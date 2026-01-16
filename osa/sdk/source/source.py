"""Source protocol for pluggable data sources."""

from collections.abc import AsyncIterator
from datetime import datetime
from typing import ClassVar, Protocol

from pydantic import BaseModel

from osa.sdk.source.record import UpstreamRecord


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

    def pull(
        self,
        since: datetime | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[UpstreamRecord]:
        """Pull records from an upstream source.

        This method returns an async iterator that yields records.
        Implementations should use async generators.

        Args:
            since: Only fetch records updated after this time.
            limit: Maximum number of records to fetch.

        Yields:
            UpstreamRecord for each ingested item.
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
