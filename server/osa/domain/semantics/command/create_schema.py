from datetime import datetime

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.semantics.model.value import FieldDefinition
from osa.domain.semantics.service.schema import SchemaService
from osa.domain.shared.authorization.gate import at_least
from osa.domain.shared.command import Command, CommandHandler, Result
from osa.domain.shared.model.srn import SchemaSRN


class CreateSchema(Command):
    title: str
    version: str
    fields: list[FieldDefinition]


class SchemaCreated(Result):
    srn: SchemaSRN
    title: str
    field_count: int
    created_at: datetime


class CreateSchemaHandler(CommandHandler[CreateSchema, SchemaCreated]):
    __auth__ = at_least(Role.ADMIN)
    principal: Principal
    schema_service: SchemaService

    async def run(self, cmd: CreateSchema) -> SchemaCreated:
        schema = await self.schema_service.create_schema(
            title=cmd.title,
            version=cmd.version,
            fields=cmd.fields,
        )
        return SchemaCreated(
            srn=schema.srn,
            title=schema.title,
            field_count=len(schema.fields),
            created_at=schema.created_at,
        )
