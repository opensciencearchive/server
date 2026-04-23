from datetime import datetime

from pydantic import ConfigDict, Field

from osa.domain.deposition.model.value import FileRequirements
from osa.domain.deposition.service.convention import ConventionService
from osa.domain.semantics.model.value import FieldDefinition
from osa.domain.shared.authorization.gate import public
from osa.domain.shared.command import Command, CommandHandler, Result
from osa.domain.shared.model.hook import HookDefinition
from osa.domain.shared.model.source import IngesterDefinition
from osa.domain.shared.model.srn import ConventionSRN, SchemaId, SchemaIdentifier


class CreateConvention(Command):
    model_config = ConfigDict(populate_by_name=True)

    id: SchemaIdentifier
    """Schema slug — becomes the ``<id>`` in ``schema_id = <id>@<version>``.

    A convention is a bundle of (schema + validators + file requirements), and
    the caller supplies the slug of the embedded schema here. The convention
    itself gets an opaque server-generated SRN.
    """

    title: str
    version: str
    schema_fields: list[FieldDefinition] = Field(alias="schema")
    file_requirements: FileRequirements
    description: str | None = None
    hooks: list[HookDefinition] = []
    ingester: IngesterDefinition | None = None


class ConventionCreated(Result):
    srn: ConventionSRN
    title: str
    description: str | None
    schema_id: SchemaId
    created_at: datetime


class CreateConventionHandler(CommandHandler[CreateConvention, ConventionCreated]):
    __auth__ = public()
    convention_service: ConventionService

    async def run(self, cmd: CreateConvention) -> ConventionCreated:
        convention = await self.convention_service.create_convention(
            id=cmd.id,
            title=cmd.title,
            version=cmd.version,
            schema=cmd.schema_fields,
            file_requirements=cmd.file_requirements,
            description=cmd.description,
            hooks=cmd.hooks,
            ingester=cmd.ingester,
        )
        return ConventionCreated(
            srn=convention.srn,
            title=convention.title,
            description=convention.description,
            schema_id=convention.schema_id,
            created_at=convention.created_at,
        )
