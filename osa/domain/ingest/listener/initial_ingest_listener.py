"""InitialIngestListener - triggers ingestion on server startup if configured."""

import logging
from uuid import uuid4

from osa.application.event import ServerStarted
from osa.config import Config
from osa.domain.ingest.event.ingest_requested import IngestRequested
from osa.domain.ingest.event.ingestion_run_completed import IngestionRunCompleted
from osa.domain.ingest.model.registry import IngestorRegistry
from osa.domain.shared.event import EventId, EventListener
from osa.domain.shared.outbox import Outbox

logger = logging.getLogger(__name__)


class TriggerInitialIngestion(EventListener[ServerStarted]):
    """Emits IngestRequested on server startup for ingestors with initial_run enabled."""

    config: Config
    ingestors: IngestorRegistry
    outbox: Outbox

    async def handle(self, event: ServerStarted) -> None:
        """Check each ingestor config and emit IngestRequested if initial_run is enabled."""
        for ingestor_name, ingest_config in self.config.ingestors.items():
            if ingest_config.initial_run is None:
                continue
            if not ingest_config.initial_run.enabled:
                continue

            # Verify ingestor exists in registry
            if ingestor_name not in self.ingestors:
                logger.error(
                    f"Initial ingest: ingestor '{ingestor_name}' not found in registry. "
                    f"Available: {self.ingestors.names()}"
                )
                continue

            initial_run = ingest_config.initial_run
            limit = initial_run.limit

            # Determine 'since': use config value, or look up last completed run
            since = initial_run.since
            if since is None:
                last_run = await self.outbox.find_latest(IngestionRunCompleted)
                if last_run and last_run.ingestor_name == ingestor_name:
                    since = last_run.completed_at
                    logger.debug(
                        f"Found previous run for '{ingestor_name}', using since={since}"
                    )

            logger.info(f"Initial ingest: {ingestor_name} (since={since}, limit={limit})")

            await self.outbox.append(
                IngestRequested(
                    id=EventId(uuid4()),
                    ingestor_name=ingestor_name,
                    since=since,
                    limit=limit,
                )
            )
        # Session commit handled by BackgroundWorker
