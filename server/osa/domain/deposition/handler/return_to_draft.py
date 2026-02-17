"""ReturnToDraft - handles ValidationFailed events."""

import logging

from osa.domain.deposition.port.repository import DepositionRepository
from osa.domain.shared.event import EventHandler
from osa.domain.validation.event.validation_failed import ValidationFailed

logger = logging.getLogger(__name__)


class ReturnToDraft(EventHandler[ValidationFailed]):
    """Returns a deposition to DRAFT when validation fails."""

    deposition_repo: DepositionRepository

    async def handle(self, event: ValidationFailed) -> None:
        dep = await self.deposition_repo.get(event.deposition_srn)
        if dep is None:
            logger.warning(f"Deposition not found for return_to_draft: {event.deposition_srn}")
            return

        dep.return_to_draft()
        await self.deposition_repo.save(dep)
        logger.info(
            f"Deposition {event.deposition_srn} returned to draft. Reasons: {event.reasons}"
        )
