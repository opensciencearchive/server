from typing import Any

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.deposition.service.deposition import DepositionService
from osa.domain.shared.authorization.gate import at_least
from osa.domain.shared.command import Command, CommandHandler, Result
from osa.domain.shared.model.srn import DepositionSRN


class UpdateMetadata(Command):
    srn: DepositionSRN
    metadata: dict[str, Any]


class MetadataUpdated(Result):
    pass


class UpdateMetadataHandler(CommandHandler[UpdateMetadata, MetadataUpdated]):
    __auth__ = at_least(Role.DEPOSITOR)
    principal: Principal
    deposition_service: DepositionService

    async def run(self, cmd: UpdateMetadata) -> MetadataUpdated:
        await self.deposition_service.update_metadata(cmd.srn, cmd.metadata)
        return MetadataUpdated()
