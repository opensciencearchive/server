from datetime import datetime

from osa.domain.semantics.model.value import FieldDefinition
from osa.domain.semantics.service.schema import SchemaService
from osa.domain.shared.authorization.gate import public
from osa.domain.shared.model.srn import SchemaSRN
from osa.domain.shared.query import Query, QueryHandler, Result


class GetSchema(Query):
    srn: SchemaSRN


class SchemaDetail(Result):
    srn: SchemaSRN
    title: str
    fields: list[FieldDefinition]
    created_at: datetime


class GetSchemaHandler(QueryHandler[GetSchema, SchemaDetail]):
    __auth__ = public()
    schema_service: SchemaService

    async def run(self, cmd: GetSchema) -> SchemaDetail:
        schema = await self.schema_service.get_schema(cmd.srn)
        return SchemaDetail(
            srn=schema.srn,
            title=schema.title,
            fields=schema.fields,
            created_at=schema.created_at,
        )
