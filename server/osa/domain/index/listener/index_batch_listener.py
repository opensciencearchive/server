"""IndexRecordBatch - batch processes IndexRecord events per backend."""

import logging
from collections import defaultdict

from osa.domain.index.event.index_record import IndexRecord
from osa.domain.index.model.registry import IndexRegistry
from osa.domain.shared.event import BatchEventListener

logger = logging.getLogger(__name__)


class IndexRecordBatch(BatchEventListener[IndexRecord]):
    """Batch processes IndexRecord events by grouping per backend.

    The BackgroundWorker groups IndexRecord events and calls handle_batch()
    with all events of this type. This listener further groups events by
    backend_name and calls ingest_batch() on each backend.

    This enables:
    - Efficient batch embedding generation
    - Per-backend failure isolation (handled at event level)
    - Crash-safe processing (events remain in outbox until committed)
    """

    indexes: IndexRegistry

    async def handle_batch(self, events: list[IndexRecord]) -> None:
        """Process a batch of IndexRecord events grouped by backend.

        Args:
            events: List of IndexRecord events to process

        Raises:
            Exception: If any backend fails to index (events will be retried)
        """
        if not events:
            return

        # Group events by backend
        by_backend: dict[str, list[IndexRecord]] = defaultdict(list)
        for event in events:
            by_backend[event.backend_name].append(event)

        logger.debug(
            f"Processing batch of {len(events)} IndexRecord events for {len(by_backend)} backends"
        )

        # Process each backend's batch
        for backend_name, backend_events in by_backend.items():
            backend = self.indexes.get(backend_name)
            if backend is None:
                # Graceful handling for removed backends (T027)
                # Log warning with record SRNs for visibility
                record_srns = [str(e.record_srn) for e in backend_events]
                logger.warning(
                    f"Backend '{backend_name}' not found, skipping {len(backend_events)} events. "
                    f"Records: {record_srns[:5]}{'...' if len(record_srns) > 5 else ''}"
                )
                continue

            # Prepare records for batch ingestion
            records = [(str(event.record_srn), event.metadata) for event in backend_events]

            logger.debug(
                f"Batch indexing {len(records)} records to backend '{backend_name}' "
                f"(batch efficiency: {len(records)} records in single call)"
            )

            try:
                await backend.ingest_batch(records)
                logger.info(f"Indexed batch of {len(records)} records to backend '{backend_name}'")
            except Exception as e:
                # Enhanced error with backend name and record SRNs (T025, T026)
                record_srns = [srn for srn, _ in records]
                error_context = (
                    f"Backend '{backend_name}' failed to index {len(records)} records. "
                    f"Records: {record_srns[:3]}{'...' if len(record_srns) > 3 else ''}. "
                    f"Error: {e}"
                )
                logger.error(error_context)
                # Re-raise with context so worker can record in delivery_error
                raise RuntimeError(error_context) from e
