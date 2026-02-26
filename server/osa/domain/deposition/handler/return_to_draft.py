"""ReturnToDraft - handles ValidationFailed events."""

import logging

from osa.domain.deposition.service.deposition import DepositionService
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.event import EventHandler
from osa.domain.validation.event.validation_failed import ValidationFailed

logger = logging.getLogger(__name__)


class ReturnToDraft(EventHandler[ValidationFailed]):
    """Returns a deposition to DRAFT when validation fails."""

    deposition_service: DepositionService

    async def handle(self, event: ValidationFailed) -> None:
        try:
            await self.deposition_service.return_to_draft(event.deposition_srn)
        except NotFoundError:
            logger.warning("Deposition not found for return_to_draft: %s", event.deposition_srn)
            return

        logger.info(
            "Deposition %s returned to draft. Reasons: %s",
            event.deposition_srn,
            event.reasons,
        )
