from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.deposition.service.deposition import DepositionService
from osa.domain.shared.authorization.gate import at_least
from osa.domain.shared.command import Command, CommandHandler, Result
from osa.domain.shared.model.srn import ConventionSRN, DepositionSRN


class CreateDeposition(Command):
    convention_srn: ConventionSRN


class DepositionCreated(Result):
    srn: DepositionSRN


class CreateDepositionHandler(CommandHandler[CreateDeposition, DepositionCreated]):
    __auth__ = at_least(Role.DEPOSITOR)
    principal: Principal
    deposition_service: DepositionService

    async def run(self, cmd: CreateDeposition) -> DepositionCreated:
        dep = await self.deposition_service.create(
            convention_srn=cmd.convention_srn,
            owner_id=self.principal.user_id,
        )
        return DepositionCreated(srn=dep.srn)
