import logfire

from osa.domain.deposition.event.submitted import DepositionSubmittedEvent
from osa.domain.shared.event import EventListener


class DepositionSubmittedListener(EventListener[DepositionSubmittedEvent]):
    """Triggers validation when a deposition is submitted."""

    # Note: In a real implementation, this would inject:
    # - ValidationService
    # - DepositionRepository (to get convention and payload)
    # - ConventionRepository (to get trait_srns)
    # For now, this is a stub that logs the event.

    async def handle(self, event: DepositionSubmittedEvent) -> None:
        logfire.info(
            "Deposition submitted, would trigger validation",
            deposition_id=str(event.deposition_id),
        )
        # TODO: Wire up full validation flow:
        # 1. Load deposition to get convention_srn and payload
        # 2. Load convention to get trait_srns
        # 3. Build ValidationInputs from deposition
        # 4. Call validation_service.validate_deposition(...)
        # 5. Emit ValidationStarted and ValidationCompleted events
