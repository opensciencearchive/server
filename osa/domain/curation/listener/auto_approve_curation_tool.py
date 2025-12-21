"""AutoApproveCurationTool - auto-approves depositions on validation completion."""

import logging
from uuid import uuid4

from osa.domain.curation.event.deposition_approved import DepositionApproved
from osa.domain.shared.event import EventId, EventListener
from osa.domain.shared.outbox import Outbox
from osa.domain.validation.event.validation_completed import ValidationCompleted
from osa.domain.validation.model import RunStatus

logger = logging.getLogger(__name__)


class AutoApproveCurationTool(EventListener[ValidationCompleted]):
    """Auto-approves validation and emits DepositionApproved. 0 curation = instant approve."""

    outbox: Outbox

    async def handle(self, event: ValidationCompleted) -> None:
        """Emit DepositionApproved if validation passed and no curation required."""
        # Only auto-approve if validation passed
        if event.status != RunStatus.COMPLETED:
            logger.warning(
                f"Validation failed for {event.deposition_srn}, skipping auto-approve"
            )
            return

        # TODO: Load curation config to check if manual curation is required
        curation_required = False  # False for v1
        if curation_required:
            logger.info(
                f"Curation required for {event.deposition_srn}, not auto-approving"
            )
            return

        logger.debug(f"Auto-approving deposition: {event.deposition_srn}")

        # Emit DepositionApproved
        approved = DepositionApproved(
            id=EventId(uuid4()),
            deposition_srn=event.deposition_srn,
            metadata=event.metadata,
        )

        await self.outbox.append(approved)
        # Session commit handled by BackgroundWorker

        logger.debug(f"Deposition approved: {event.deposition_srn}")
