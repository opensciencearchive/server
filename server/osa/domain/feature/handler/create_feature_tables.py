"""CreateFeatureTables — creates feature tables when a convention is registered."""

import logging
from uuid import uuid4

from osa.domain.deposition.event.convention_registered import ConventionRegistered
from osa.domain.feature.event.convention_ready import ConventionReady
from osa.domain.feature.service.feature import FeatureService
from osa.domain.shared.event import EventHandler, EventId
from osa.domain.shared.outbox import Outbox

logger = logging.getLogger(__name__)


class CreateFeatureTables(EventHandler[ConventionRegistered]):
    """Creates feature tables for each hook and emits ConventionReady.

    Part of the convention initialization chain:
    ConventionRegistered → CreateFeatureTables → ConventionReady
    """

    feature_service: FeatureService
    outbox: Outbox

    async def handle(self, event: ConventionRegistered) -> None:
        for hook_snapshot in event.hooks:
            logger.info(
                "Creating feature table: hook=%s convention=%s",
                hook_snapshot.name,
                event.convention_srn,
            )
            await self.feature_service.create_table_from_snapshot(hook_snapshot)

        await self.outbox.append(
            ConventionReady(
                id=EventId(uuid4()),
                convention_srn=event.convention_srn,
            )
        )
        logger.info("Convention ready: %s", event.convention_srn)
