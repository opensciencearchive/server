import logfire

from osa.domain.deposition.service.deposition import DepositionService
from osa.domain.shared.command import Command, CommandHandler, Result


class CreateDeposition(Command): ...


class DepositionCreated(Result): ...


class CreateDepositionHandler(CommandHandler[CreateDeposition, DepositionCreated]):
    deposition_service: DepositionService

    def run(self, cmd: CreateDeposition) -> DepositionCreated:
        # TODO: Logfire span
        logfire.info("Deposition started", cmd=cmd)

        return DepositionCreated()
