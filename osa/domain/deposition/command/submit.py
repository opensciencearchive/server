import logfire
from uuid import uuid4

from osa.domain.deposition.event.submitted import DepositionSubmittedEvent
from osa.domain.deposition.service.deposition import DepositionService
from osa.domain.shared.command import Command, CommandHandler, Result
from osa.domain.shared.event import EventId
from osa.domain.shared.model.srn import DepositionSRN
from osa.domain.shared.outbox import Outbox


class SubmitDeposition(Command):
    srn: DepositionSRN


class DepositionSubmitted(Result):
    pass


class SubmitDepositionHandler(CommandHandler[SubmitDeposition, DepositionSubmitted]):
    deposition_service: DepositionService
    outbox: Outbox

    async def run(self, cmd: SubmitDeposition) -> DepositionSubmitted:
        with logfire.span("SubmitDeposition"):
            # TODO: Domain logic via service (state transition)
            # self.deposition_service.submit(cmd.srn)

            # Append event to outbox for reliable delivery
            event = DepositionSubmittedEvent(
                id=EventId(uuid4()),
                deposition_id=cmd.srn,
                metadata={},  # Empty metadata for direct submission (not from ingest)
            )
            await self.outbox.append(event)

            logfire.info("Deposition submitted event saved to outbox", deposition_id=str(cmd.srn))

            return DepositionSubmitted()
