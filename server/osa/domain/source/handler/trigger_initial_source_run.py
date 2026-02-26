"""TriggerInitialSourceRun - triggers source pull when feature tables are ready."""

import logging
from uuid import uuid4

from osa.domain.deposition.service.convention import ConventionService
from osa.domain.feature.event.convention_ready import ConventionReady
from osa.domain.shared.event import EventHandler, EventId
from osa.domain.shared.outbox import Outbox
from osa.domain.source.event.source_requested import SourceRequested

logger = logging.getLogger(__name__)


class TriggerInitialSourceRun(EventHandler[ConventionReady]):
    """Emits SourceRequested when a convention with initial_run is ready.

    Part of the convention initialization chain:
    ConventionRegistered → CreateFeatureTables → ConventionReady → TriggerInitialSourceRun
    """

    convention_service: ConventionService
    outbox: Outbox

    async def handle(self, event: ConventionReady) -> None:
        conv = await self.convention_service.get_convention(event.convention_srn)

        if conv.source is None or conv.source.initial_run is None:
            return

        logger.info("Source deploy trigger: convention=%s", conv.srn)

        await self.outbox.append(
            SourceRequested(
                id=EventId(uuid4()),
                convention_srn=conv.srn,
                limit=conv.source.initial_run.limit,
            )
        )
