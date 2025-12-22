"""IngestListener - handles IngestRequested events."""

import logging
from datetime import UTC, datetime
from uuid import uuid4

from osa.config import Config
from osa.domain.deposition.event.submitted import DepositionSubmittedEvent
from osa.domain.ingest.event.ingest_requested import IngestRequested
from osa.domain.ingest.event.ingestion_run_completed import IngestionRunCompleted
from osa.domain.ingest.model.registry import IngestorRegistry
from osa.domain.shared.event import EventId, EventListener
from osa.domain.shared.model.srn import DepositionSRN, Domain, LocalId
from osa.domain.shared.outbox import Outbox

logger = logging.getLogger(__name__)


class IngestFromUpstream(EventListener[IngestRequested]):
    """Pulls from upstream source and creates depositions."""

    ingestors: IngestorRegistry
    outbox: Outbox
    config: Config

    async def handle(self, event: IngestRequested) -> None:
        """Pull records from ingestor and emit DepositionSubmitted for each."""
        ingestor = self.ingestors.get(event.ingestor_name)
        if not ingestor:
            logger.error(f"Unknown ingestor: {event.ingestor_name}")
            return

        started_at = datetime.now(UTC)
        logger.info(
            f"Starting ingest from {event.ingestor_name}, "
            f"since={event.since}, limit={event.limit}"
        )

        domain = Domain(self.config.server.domain)
        count = 0

        async for record in ingestor.pull(since=event.since, limit=event.limit):
            # Create a deposition SRN for this record
            dep_srn = DepositionSRN(
                domain=domain,
                id=LocalId(str(uuid4())),
            )

            # Emit DepositionSubmitted with the record metadata
            submitted_event = DepositionSubmittedEvent(
                id=EventId(uuid4()),
                deposition_id=dep_srn,
                metadata=record.metadata,
            )
            await self.outbox.append(submitted_event)
            count += 1

            # Log record metadata
            title = record.metadata.get("title", "")[:60]
            logger.info(f"  [{record.source_id}] {title}...")

        completed_at = datetime.now(UTC)
        logger.info(f"Ingest completed: {count} records from {event.ingestor_name}")

        # Emit completion event for tracking
        await self.outbox.append(
            IngestionRunCompleted(
                id=EventId(uuid4()),
                ingestor_name=event.ingestor_name,
                source_type=ingestor.source_type,
                started_at=started_at,
                completed_at=completed_at,
                record_count=count,
                since=event.since,
                limit=event.limit,
            )
        )
        # Session commit handled by BackgroundWorker
