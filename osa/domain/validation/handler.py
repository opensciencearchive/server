import asyncio
import logfire

from osa.domain.deposition.event.submitted import DepositionSubmittedEvent
from osa.domain.shared.event import EventListener
from osa.domain.shared.model.srn import Domain, EventSRN, LocalId, ValidationRunSRN
from osa.domain.shared.port.event_bus import EventBus
from osa.domain.validation.event.validation_completed import ValidationCompleted
from osa.domain.validation.model import RunStatus


class ValidationHandler(EventListener[DepositionSubmittedEvent]):
    """Stub handler that simulates validation. Replace with real ValidationService wiring."""

    def __init__(self, event_bus: EventBus) -> None:
        self.event_bus = event_bus

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

            # Emit ValidationCompleted
            completed_event = ValidationCompleted(
                srn=EventSRN.parse("urn:osa:localhost:evt:val-completed"),
                validation_run_srn=validation_run_srn,
                deposition_srn=event.deposition_id,
                status=RunStatus.COMPLETED,
                results=[],
            )
            await self.event_bus.publish(completed_event)
            logfire.info("Validation completed event published")
