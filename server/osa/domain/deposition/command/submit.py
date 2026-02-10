from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.deposition.service.deposition import DepositionService
from osa.domain.shared.authorization.gate import at_least
from osa.domain.shared.command import Command, CommandHandler, Result
from osa.domain.shared.model.srn import DepositionSRN


class SubmitDeposition(Command):
    srn: DepositionSRN


class DepositionSubmitted(Result):
    pass


class SubmitDepositionHandler(CommandHandler[SubmitDeposition, DepositionSubmitted]):
    __auth__ = at_least(Role.DEPOSITOR)
    principal: Principal
    deposition_service: DepositionService

    async def run(self, cmd: SubmitDeposition) -> DepositionSubmitted:
        await self.deposition_service.submit(cmd.srn)
        return DepositionSubmitted()
