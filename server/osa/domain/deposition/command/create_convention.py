from datetime import datetime

from pydantic import ConfigDict, Field

from osa.domain.deposition.model.value import FileRequirements
from osa.domain.deposition.service.convention import ConventionService
from osa.domain.semantics.model.value import FieldDefinition
from osa.domain.shared.authorization.gate import public
from osa.domain.shared.command import Command, CommandHandler, Result
from osa.domain.shared.model.hook import HookDefinition
from osa.domain.shared.model.source import SourceDefinition
from osa.domain.shared.model.srn import ConventionSRN, SchemaSRN


class CreateConvention(Command):
    model_config = ConfigDict(populate_by_name=True)

    title: str
    version: str
    schema_fields: list[FieldDefinition] = Field(alias="schema")
    file_requirements: FileRequirements
    description: str | None = None
    hooks: list[HookDefinition] = []
    source: SourceDefinition | None = None


class ConventionCreated(Result):
    srn: ConventionSRN
    title: str
    description: str | None
    schema_srn: SchemaSRN
    created_at: datetime


class CreateConventionHandler(CommandHandler[CreateConvention, ConventionCreated]):
    __auth__ = public()
    convention_service: ConventionService

    async def run(self, cmd: CreateConvention) -> ConventionCreated:
        convention = await self.convention_service.create_convention(
            title=cmd.title,
            version=cmd.version,
            schema=cmd.schema_fields,
            file_requirements=cmd.file_requirements,
            description=cmd.description,
            hooks=cmd.hooks,
            source=cmd.source,
        )
        return ConventionCreated(
            srn=convention.srn,
            title=convention.title,
            description=convention.description,
            schema_srn=convention.schema_srn,
            created_at=convention.created_at,
        )
