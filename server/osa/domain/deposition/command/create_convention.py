from datetime import datetime

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.deposition.model.value import FileRequirements
from osa.domain.deposition.service.convention import ConventionService
from osa.domain.shared.authorization.gate import at_least
from osa.domain.shared.command import Command, CommandHandler, Result
from osa.domain.shared.model.hook import HookDefinition
from osa.domain.shared.model.srn import ConventionSRN, SchemaSRN


class CreateConvention(Command):
    title: str
    version: str
    schema_srn: SchemaSRN
    file_requirements: FileRequirements
    description: str | None = None
    hooks: list[HookDefinition] = []


class ConventionCreated(Result):
    srn: ConventionSRN
    title: str
    description: str | None
    schema_srn: SchemaSRN
    created_at: datetime


class CreateConventionHandler(CommandHandler[CreateConvention, ConventionCreated]):
    __auth__ = at_least(Role.ADMIN)
    principal: Principal
    convention_service: ConventionService

    async def run(self, cmd: CreateConvention) -> ConventionCreated:
        convention = await self.convention_service.create_convention(
            title=cmd.title,
            version=cmd.version,
            schema_srn=cmd.schema_srn,
            file_requirements=cmd.file_requirements,
            description=cmd.description,
            hooks=cmd.hooks,
        )
        return ConventionCreated(
            srn=convention.srn,
            title=convention.title,
            description=convention.description,
            schema_srn=convention.schema_srn,
            created_at=convention.created_at,
        )
