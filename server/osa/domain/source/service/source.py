"""SourceService - orchestrates pulling from data sources."""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

from osa.domain.deposition.event.submitted import DepositionSubmittedEvent
from osa.domain.shared.event import EventId
from osa.domain.shared.model.srn import DepositionSRN, Domain, LocalId
from osa.domain.shared.outbox import Outbox
from osa.domain.shared.service import Service
from osa.domain.source.event.source_run_completed import SourceRunCompleted
from osa.domain.source.model.registry import SourceRegistry

logger = logging.getLogger(__name__)


@dataclass
class SourceResult:
    """Result of a source run."""

    source_name: str
    record_count: int
    started_at: datetime
    completed_at: datetime


class SourceService(Service):
    """Orchestrates pulling records from sources and emitting deposition events.

    This service encapsulates the business logic for pulling from sources that was
    previously embedded in the PullFromSource listener. It can be called from multiple
    entry points (event listeners, CLI commands, scheduled jobs).
    """

    sources: SourceRegistry
    outbox: Outbox
    node_domain: Domain

    async def run_source(
        self,
        source_name: str,
        since: datetime | None = None,
        limit: int | None = None,
    ) -> SourceResult:
        """Pull records from a source and emit DepositionSubmitted events.

        Args:
            source_name: Name of the source to use.
            since: Only fetch records updated after this time.
            limit: Maximum number of records to fetch.

        Returns:
            SourceResult with run statistics.

        Raises:
            ValueError: If the source is not found.
        """
        source = self.sources.get(source_name)
        if not source:
            raise ValueError(f"Unknown source: {source_name}")

        started_at = datetime.now(UTC)
        logger.info(f"Starting pull from {source_name}, since={since}, limit={limit}")

        count = 0
        async for record in source.pull(since=since, limit=limit):
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
        logger.info(f"Pull completed: {count} records from {source_name}")

        # Emit completion event for tracking
        await self.outbox.append(
            SourceRunCompleted(
                id=EventId(uuid4()),
                source_name=source_name,
                source_type=source.name,
                started_at=started_at,
                completed_at=completed_at,
                record_count=count,
                since=since,
                limit=limit,
            )
        )

        return SourceResult(
            source_name=source_name,
            record_count=count,
            started_at=started_at,
            completed_at=completed_at,
        )
