"""TriggerSourceOnDeploy - triggers source pull when a convention with a source is deployed."""

import logging
from uuid import uuid4

from osa.domain.deposition.event.convention_registered import ConventionRegistered
from osa.domain.deposition.service.convention import ConventionService
from osa.domain.shared.event import EventHandler, EventId
from osa.domain.shared.outbox import Outbox
from osa.domain.source.event.source_requested import SourceRequested

logger = logging.getLogger(__name__)


class TriggerSourceOnDeploy(EventHandler[ConventionRegistered]):
    """Emits SourceRequested when a convention with initial_run is deployed."""

    convention_service: ConventionService
    outbox: Outbox

    async def handle(self, event: ConventionRegistered) -> None:
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
