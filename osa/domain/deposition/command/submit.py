import logfire

from osa.domain.deposition.event.submitted import DepositionSubmittedEvent
from osa.domain.deposition.service.deposition import DepositionService
from osa.domain.shared.command import Command, CommandHandler, Result
from osa.domain.shared.model.srn import DepositionSRN
from osa.domain.shared.port.event_bus import EventBus


class SubmitDeposition(Command):
    srn: DepositionSRN


class DepositionSubmitted(Result):
    pass


class SubmitDepositionHandler(CommandHandler[SubmitDeposition, DepositionSubmitted]):
    deposition_service: DepositionService
    event_bus: EventBus

    async def run(self, cmd: SubmitDeposition) -> DepositionSubmitted:
        with logfire.span("SubmitDeposition"):
            # TODO: Domain logic via service (state transition)
            # self.deposition_service.submit(cmd.srn)
            
            # Publish Event
            # Note: Ideally this happens in the Service or Aggregate, 
            # or via Outbox. For prototype, publishing here is fine.
            from osa.domain.shared.model.srn import EventSRN
            event = DepositionSubmittedEvent(
                srn=EventSRN.parse("urn:osa:mock:evt:submitted"),  # Mock SRN
                deposition_id=cmd.srn
            )
            await self.event_bus.publish(event)
            
            logfire.info("Deposition submitted event published", deposition_id=str(cmd.srn))

            return DepositionSubmitted()
