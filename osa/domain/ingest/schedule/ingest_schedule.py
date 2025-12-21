"""IngestSchedule - scheduled task that emits IngestRequested events."""

import logging
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from osa.domain.ingest.event.ingest_requested import IngestRequested
from osa.domain.ingest.event.ingestion_run_completed import IngestionRunCompleted
from osa.domain.shared.event import EventId, Schedule
from osa.domain.shared.outbox import Outbox

logger = logging.getLogger(__name__)


@dataclass
class IngestSchedule(Schedule):
    """Scheduled task that emits IngestRequested events.

    Looks up the last completed ingestion run to determine the `since` timestamp,
    then emits an IngestRequested event to trigger a new ingestion.
    """

    outbox: Outbox

    async def run(self, **params: Any) -> None:
        """Emit an IngestRequested event for the given ingestor.

        Params:
            ingestor_name: Key into config.ingestors dict (e.g., "geo")
            limit: Optional limit on records to fetch
        """
        ingestor_name: str = params["ingestor_name"]
        limit: int | None = params.get("limit")

        # Look up last completed run for this ingestor
        last_run = await self.outbox.find_latest(IngestionRunCompleted)

        # Only use last_run if it's for the same ingestor
        since = None
        if last_run is not None and last_run.ingestor_name == ingestor_name:
            since = last_run.completed_at

        logger.info(f"Scheduled ingest: {ingestor_name} (since={since}, limit={limit})")

        await self.outbox.append(
            IngestRequested(
                id=EventId(uuid4()),
                ingestor_name=ingestor_name,
                since=since,
                limit=limit,
            )
        )
