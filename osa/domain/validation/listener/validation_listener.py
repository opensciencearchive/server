"""ValidationListener - handles DepositionSubmitted events."""

import logging
from uuid import uuid4

from osa.config import Config
from osa.domain.deposition.event.submitted import DepositionSubmittedEvent
from osa.domain.shared.event import EventId, EventListener
from osa.domain.shared.model.srn import Domain, LocalId, ValidationRunSRN
from osa.domain.shared.outbox import Outbox
from osa.domain.validation.event.validation_completed import ValidationCompleted
from osa.domain.validation.model import RunStatus

logger = logging.getLogger(__name__)


class ValidationListener(EventListener[DepositionSubmittedEvent]):
    """Runs validation on depositions. 0 validators = instant pass."""

    outbox: Outbox
    config: Config

    async def handle(self, event: DepositionSubmittedEvent) -> None:
        """Run validators and emit ValidationCompleted."""
        logger.debug(f"Validating deposition: {event.deposition_id}")

        domain = Domain(self.config.server.domain)

        # Create validation run SRN
        val_run_srn = ValidationRunSRN(
            domain=domain,
            id=LocalId(str(uuid4())),
        )

        # Run validators (none configured = instant pass)
        # TODO: Load configured validators from config
        validators: list = []  # Empty for v1
        if not validators:
            logger.debug("No validators configured, instant pass")
            status = RunStatus.COMPLETED
            results = []
        else:
            # TODO: Actually run validators
            status = RunStatus.COMPLETED
            results = []

        # Emit ValidationCompleted
        completed = ValidationCompleted(
            id=EventId(uuid4()),
            validation_run_srn=val_run_srn,
            deposition_srn=event.deposition_id,
            status=status,
            results=results,
            metadata=event.metadata,  # Pass through
        )

        await self.outbox.append(completed)
        # Session commit handled by BackgroundWorker

        logger.debug(f"Validation completed for: {event.deposition_id}")
