"""CreateFeatureTables — creates feature tables when a convention is registered."""

import logging

from osa.domain.deposition.event.convention_registered import ConventionRegistered
from osa.domain.feature.service.feature import FeatureService
from osa.domain.shared.error import ConflictError
from osa.domain.shared.event import EventHandler

logger = logging.getLogger(__name__)


class CreateFeatureTables(EventHandler[ConventionRegistered]):
    """Creates feature tables for each hook declared on a registered convention.

    Readiness is not signalled via a follow-on event — consumers check the
    ``feature_tables`` + ``metadata_tables`` catalogs at read time instead
    (research.md §11).
    """

    feature_service: FeatureService

    async def handle(self, event: ConventionRegistered) -> None:
        for hook in event.hooks:
            logger.info(
                "Creating feature table: hook=%s convention=%s",
                hook.name,
                event.convention_srn,
            )
            try:
                await self.feature_service.create_table(hook)
            except ConflictError:
                logger.warning(
                    "Feature table already exists, skipping: hook=%s convention=%s",
                    hook.name,
                    event.convention_srn,
                )
