"""IndexService - orchestrates indexing of records into storage backends."""

import logging

from osa.domain.index.model.registry import IndexRegistry
from osa.domain.shared.service import Service

logger = logging.getLogger(__name__)


class IndexService(Service):
    """Service for index-related operations.

    Note: Direct indexing (index_record, flush_all) has been replaced by the
    event-driven approach using FanOutToIndexBackends and IndexRecordBatch
    listeners. This service is retained for query operations and future
    index management commands.
    """

    indexes: IndexRegistry

    async def get_count(self, backend_name: str) -> int | None:
        """Get the document count for a specific backend.

        Args:
            backend_name: Name of the backend to query.

        Returns:
            Document count, or None if backend not found.
        """
        backend = self.indexes.get(backend_name)
        if backend is None:
            return None
        return await backend.count()

    async def check_health(self, backend_name: str) -> bool | None:
        """Check health of a specific backend.

        Args:
            backend_name: Name of the backend to check.

        Returns:
            True if healthy, False if unhealthy, None if backend not found.
        """
        backend = self.indexes.get(backend_name)
        if backend is None:
            return None
        return await backend.health()
