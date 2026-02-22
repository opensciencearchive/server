"""TriggerInitialSourceRun - triggers source pull on server startup for conventions with sources."""

import logging
from uuid import uuid4

from osa.application.event import ServerStarted
from osa.domain.deposition.service.convention import ConventionService
from osa.domain.shared.event import EventHandler, EventId
from osa.domain.shared.outbox import Outbox
from osa.domain.source.event.source_requested import SourceRequested
from osa.domain.source.event.source_run_completed import SourceRunCompleted

logger = logging.getLogger(__name__)


class TriggerInitialSourceRun(EventHandler[ServerStarted]):
    """Emits SourceRequested on server startup for conventions that have initial_run configured."""

    convention_service: ConventionService
    outbox: Outbox

    async def handle(self, event: ServerStarted) -> None:
        """Check conventions for source with initial_run and emit SourceRequested for each."""
        conventions = await self.convention_service.list_conventions_with_source()
        logger.info(
            "TriggerInitialSourceRun: found %d conventions with sources",
            len(conventions),
        )

        for conv in conventions:
            if conv.source is None:
                continue

            if conv.source.initial_run is None:
                continue

            # Check if initial run already completed for this convention
            last_run = await self.outbox.find_latest(SourceRunCompleted)
            if last_run and last_run.convention_srn == conv.srn:
                logger.debug(
                    "Initial source run: skipping %s - already completed at %s",
                    conv.srn,
                    last_run.completed_at,
                )
                continue

            logger.info("Initial source run: convention=%s", conv.srn)

            await self.outbox.append(
                SourceRequested(
                    id=EventId(uuid4()),
                    convention_srn=conv.srn,
                    limit=conv.source.initial_run.limit,
                )
            )
