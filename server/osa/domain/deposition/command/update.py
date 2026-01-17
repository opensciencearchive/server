import logfire

from osa.domain.deposition.service.deposition import DepositionService
from osa.domain.shared.command import Command, CommandHandler, Result


class UpdateDeposition(Command): ...


class DepositionUpdated(Result): ...


class UpdateDepositionHandler(CommandHandler[UpdateDeposition, DepositionUpdated]):
    deposition_service: DepositionService

    async def run(self, cmd: UpdateDeposition) -> DepositionUpdated:
        # TODO: Logfire span
        logfire.info("Deposition updated", cmd=cmd)

        return DepositionUpdated()
