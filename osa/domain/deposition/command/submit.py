import logfire

from osa.domain.deposition.service.deposition import DepositionService
from osa.domain.shared.command import Command, CommandHandler, Result


class SubmitDeposition(Command): ...


class DepositionSubmitted(Result): ...


class SubmitDepositionHandler(CommandHandler[SubmitDeposition, DepositionSubmitted]):
    deposition_service: DepositionService

    def run(self, cmd: SubmitDeposition) -> DepositionSubmitted:
        # TODO: Logfire span
        logfire.info("Deposition submitted", cmd=cmd)

        return DepositionSubmitted()
