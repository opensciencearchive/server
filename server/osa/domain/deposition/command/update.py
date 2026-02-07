import logfire

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.deposition.service.deposition import DepositionService
from osa.domain.shared.authorization.gate import at_least
from osa.domain.shared.command import Command, CommandHandler, Result


class UpdateDeposition(Command): ...


class DepositionUpdated(Result): ...


class UpdateDepositionHandler(CommandHandler[UpdateDeposition, DepositionUpdated]):
    __auth__ = at_least(Role.DEPOSITOR)
    principal: Principal
    deposition_service: DepositionService

    async def run(self, cmd: UpdateDeposition) -> DepositionUpdated:
        # TODO: Logfire span
        logfire.info("Deposition updated", cmd=cmd)

        return DepositionUpdated()
