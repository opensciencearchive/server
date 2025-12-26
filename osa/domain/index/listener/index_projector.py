"""IndexProjector - indexes published records into storage backends."""

import logging

from osa.domain.index.model.registry import IndexRegistry
from osa.domain.record.event.record_published import RecordPublished
from osa.domain.shared.event import EventListener

logger = logging.getLogger(__name__)


class ProjectNewRecordToIndexes(EventListener[RecordPublished]):
    """Projects published records into index backends."""

    indexes: IndexRegistry

    async def handle(self, event: RecordPublished) -> None:
        """Index record into all configured backends."""
        srn_str = str(event.record_srn)

        # Index into all configured backends
        for name, backend in self.indexes.items():
            try:
                await backend.ingest(srn_str, event.metadata)
                logger.debug(f"Indexed {srn_str} into backend '{name}'")
            except Exception as e:
                logger.error(f"Failed to index {srn_str} into '{name}': {e}")
