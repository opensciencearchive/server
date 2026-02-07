from typing import Any

import logfire

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.deposition.service.deposition import DepositionService
from osa.domain.shared.authorization.policy import requires_role
from osa.domain.shared.command import Command, CommandHandler, Result
from osa.domain.shared.model.srn import DepositionSRN


class CreateDeposition(Command):
    metadata: dict[str, Any] = {}


class DepositionCreated(Result):
    srn: DepositionSRN


class CreateDepositionHandler(CommandHandler[CreateDeposition, DepositionCreated]):
    __auth__ = requires_role(Role.DEPOSITOR)
    _principal: Principal | None = None
    deposition_service: DepositionService

    async def run(self, cmd: CreateDeposition) -> DepositionCreated:
        # TODO: Logfire span
        logfire.info("Deposition started", cmd=cmd)

        # Mock SRN for now since we don't have ID generation wired up
        mock_srn = DepositionSRN.parse("urn:osa:mock-node:dep:mock-id")

        return DepositionCreated(srn=mock_srn)
