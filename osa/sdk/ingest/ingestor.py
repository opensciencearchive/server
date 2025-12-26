"""Ingestor protocol for pluggable data source ingestors."""

from collections.abc import AsyncIterator
from datetime import datetime
from typing import ClassVar, Protocol

from pydantic import BaseModel

from osa.sdk.ingest.record import UpstreamRecord


class Ingestor(Protocol):
    """Protocol for pluggable data source ingestors.

    Implement this protocol to create custom ingestors
    for different data sources (e.g., GEO, ENA, Zenodo).

    Class attributes:
        name: Unique identifier for this ingestor (e.g., 'geo-entrez').
            Must match the entry point name.
        config_class: Pydantic model for validating configuration.
    """

    name: ClassVar[str]
    config_class: ClassVar[type[BaseModel]]

    def __init__(self, config: BaseModel) -> None:
        """Initialize the ingestor with validated configuration."""
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
