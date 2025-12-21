import asyncio
from uuid import uuid4

import logfire

from osa.domain.deposition.event.submitted import DepositionSubmittedEvent
from osa.domain.shared.event import EventId, EventListener
from osa.domain.shared.model.srn import Domain, LocalId, ValidationRunSRN
from osa.domain.shared.outbox import Outbox
from osa.domain.validation.event.validation_completed import ValidationCompleted
from osa.domain.validation.model import RunStatus


class BeginMockValidation(EventListener[DepositionSubmittedEvent]):
    """Stub handler that simulates validation. Replace with real ValidationService wiring."""

    outbox: Outbox

    async def handle(self, event: DepositionSubmittedEvent) -> None:
        with logfire.span("ValidationHandler"):
            logfire.info(
                "Received DepositionSubmitted, starting validation simulation",
                deposition_id=str(event.deposition_id),
            )

            # Simulate async work
            await asyncio.sleep(1)

            # Create a mock validation run SRN
            validation_run_srn = ValidationRunSRN(
                domain=Domain("localhost"),
                id=LocalId("mock-validation-run"),
                version=None,
            )

            # Emit ValidationCompleted via outbox
            completed_event = ValidationCompleted(
                id=EventId(uuid4()),
                validation_run_srn=validation_run_srn,
                deposition_srn=event.deposition_id,
                status=RunStatus.COMPLETED,
                results=[],
                metadata=event.metadata,  # Pass through the original metadata
            )
            await self.outbox.append(completed_event)
            logfire.info("Validation completed event saved to outbox")
