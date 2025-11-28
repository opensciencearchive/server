import logfire

from osa.domain.deposition.service.deposition import DepositionService
from osa.domain.shared.command import Command, CommandHandler, Result
from osa.domain.shared.model.srn import DepositionSRN


class SubmitDeposition(Command):
    srn: DepositionSRN


class DepositionSubmitted(Result):
    pass


class SubmitDepositionHandler(CommandHandler[SubmitDeposition, DepositionSubmitted]):
    deposition_service: DepositionService

    def run(self, cmd: SubmitDeposition) -> DepositionSubmitted:
        # TODO: Logfire span
        logfire.info("Deposition submitted", cmd=cmd)

        return DepositionSubmitted()
