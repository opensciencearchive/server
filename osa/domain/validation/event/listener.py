import logfire

from osa.domain.deposition.event.submitted import DepositionSubmittedEvent
from osa.domain.shared.event import EventListener


class DepositionSubmittedListener(EventListener[DepositionSubmittedEvent]):
    """Triggers validation when a deposition is submitted."""

    async def handle(self, event: DepositionSubmittedEvent) -> None:
        logfire.info(
            "Deposition submitted, would trigger validation",
            deposition_id=str(event.deposition_id),
        )
        # TODO: Wire up full validation flow:
        # 1. Load deposition to get payload
        # 2. Build ValidationInputs from deposition
        # 3. Call validation_service.validate_deposition(...)
        # 4. Emit ValidationStarted and ValidationCompleted events
