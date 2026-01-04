"""IngestService - orchestrates ingestion from upstream sources."""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

from osa.domain.deposition.event.submitted import DepositionSubmittedEvent
from osa.domain.ingest.event.ingestion_run_completed import IngestionRunCompleted
from osa.domain.ingest.model.registry import IngestorRegistry
from osa.domain.shared.event import EventId
from osa.domain.shared.model.srn import DepositionSRN, Domain, LocalId
from osa.domain.shared.outbox import Outbox
from osa.domain.shared.service import Service

logger = logging.getLogger(__name__)


@dataclass
class IngestResult:
    """Result of an ingestion run."""

    ingestor_name: str
    record_count: int
    started_at: datetime
    completed_at: datetime


class IngestService(Service):
    """Orchestrates pulling records from upstream ingestors and emitting deposition events.

    This service encapsulates the business logic for ingestion that was previously
    embedded in the IngestFromUpstream listener. It can be called from multiple
    entry points (event listeners, CLI commands, scheduled jobs).
    """

    ingestors: IngestorRegistry
    outbox: Outbox
    node_domain: Domain

    async def run_ingest(
        self,
        ingestor_name: str,
        since: datetime | None = None,
        limit: int | None = None,
    ) -> IngestResult:
        """Pull records from an ingestor and emit DepositionSubmitted events.

        Args:
            ingestor_name: Name of the ingestor to use.
            since: Only fetch records updated after this time.
            limit: Maximum number of records to fetch.

        Returns:
            IngestResult with ingestion statistics.

        Raises:
            ValueError: If the ingestor is not found.
        """
        ingestor = self.ingestors.get(ingestor_name)
        if not ingestor:
            raise ValueError(f"Unknown ingestor: {ingestor_name}")

        started_at = datetime.now(UTC)
        logger.info(f"Starting ingest from {ingestor_name}, since={since}, limit={limit}")

        count = 0
        async for record in ingestor.pull(since=since, limit=limit):
            # Create a deposition SRN for this record
            dep_srn = DepositionSRN(
                domain=self.node_domain,
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
        logger.info(f"Ingest completed: {count} records from {ingestor_name}")

        # Emit completion event for tracking
        await self.outbox.append(
            IngestionRunCompleted(
                id=EventId(uuid4()),
                ingestor_name=ingestor_name,
                source_type=ingestor.name,
                started_at=started_at,
                completed_at=completed_at,
                record_count=count,
                since=since,
                limit=limit,
            )
        )

        return IngestResult(
            ingestor_name=ingestor_name,
            record_count=count,
            started_at=started_at,
            completed_at=completed_at,
        )
