"""ValidateDeposition - handles DepositionSubmitted events."""

import logging
from uuid import uuid4

from osa.domain.deposition.event.submitted import DepositionSubmittedEvent
from osa.domain.shared.event import EventHandler, EventId
from osa.domain.shared.outbox import Outbox
from osa.domain.validation.event.validation_completed import ValidationCompleted
from osa.domain.validation.event.validation_failed import ValidationFailed
from osa.domain.validation.model import RunStatus
from osa.domain.validation.service.validation import ValidationService

logger = logging.getLogger(__name__)


class ValidateDeposition(EventHandler[DepositionSubmittedEvent]):
    """Runs hooks on depositions. 0 hooks = instant pass."""

    outbox: Outbox
    validation_service: ValidationService

    async def handle(self, event: DepositionSubmittedEvent) -> None:
        """Run hooks and emit ValidationCompleted or ValidationFailed."""
        logger.debug(f"Validating deposition: {event.deposition_id}")

        try:
            run, hook_results = await self.validation_service.validate_deposition(
                deposition_srn=event.deposition_id,
                convention_srn=event.convention_srn,
                metadata=event.metadata,
                hooks=event.hooks,
                files_dir=event.files_dir,
            )
        except ValueError:
            logger.error(f"Validation setup failed for: {event.deposition_id}")
            return

        if run.status in (RunStatus.FAILED, RunStatus.REJECTED):
            reasons = [
                r.error_message or r.rejection_reason or "Unknown"
                for r in hook_results
                if r.error_message or r.rejection_reason
            ]
            failed = ValidationFailed(
                id=EventId(uuid4()),
                deposition_srn=event.deposition_id,
                convention_srn=event.convention_srn,
                status=run.status,
                reasons=reasons,
            )
            await self.outbox.append(failed)
            logger.info(f"Validation failed for: {event.deposition_id}")
        else:
            completed = ValidationCompleted(
                id=EventId(uuid4()),
                validation_run_srn=run.srn,
                deposition_srn=event.deposition_id,
                convention_srn=event.convention_srn,
                status=run.status,
                hook_results=[r.model_dump() for r in hook_results],
                metadata=event.metadata,
                hooks=event.hooks,
                files_dir=event.files_dir,
            )
            await self.outbox.append(completed)
            logger.debug(f"Validation completed for: {event.deposition_id}")
