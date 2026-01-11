"""InitialSourceListener - triggers source pull on server startup if configured."""

import logging
from uuid import uuid4

from osa.application.event import ServerStarted
from osa.config import Config
from osa.domain.shared.event import EventId, EventListener
from osa.domain.shared.outbox import Outbox
from osa.domain.source.event.source_requested import SourceRequested
from osa.domain.source.event.source_run_completed import SourceRunCompleted
from osa.domain.source.model.registry import SourceRegistry

logger = logging.getLogger(__name__)


class TriggerInitialSourceRun(EventListener[ServerStarted]):
    """Emits SourceRequested on server startup for sources with initial_run enabled."""

    config: Config
    sources: SourceRegistry
    outbox: Outbox

    async def handle(self, event: ServerStarted) -> None:
        """Check each source config and emit SourceRequested if initial_run is enabled."""
        for source_config in self.config.sources:
            if source_config.initial_run is None:
                continue
            if not source_config.initial_run.enabled:
                continue

            source_name = source_config.name

            # Verify source exists in registry
            if source_name not in self.sources:
                logger.error(
                    f"Initial source run: source '{source_name}' not found in registry. "
                    f"Available: {self.sources.names()}"
                )
                continue

            # Check if initial run already completed for this source
            last_run = await self.outbox.find_latest(SourceRunCompleted)
            if last_run and last_run.source_name == source_name:
                logger.debug(
                    f"Initial source run: skipping '{source_name}' - "
                    f"already completed at {last_run.completed_at}"
                )
                continue

            initial_run = source_config.initial_run
            limit = initial_run.limit
            since = initial_run.since

            logger.info(f"Initial source run: {source_name} (since={since}, limit={limit})")

            await self.outbox.append(
                SourceRequested(
                    id=EventId(uuid4()),
                    source_name=source_name,
                    since=since,
                    limit=limit,
                )
            )
        # Session commit handled by BackgroundWorker
