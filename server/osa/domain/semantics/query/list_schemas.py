from datetime import datetime

from pydantic import BaseModel

from osa.domain.semantics.service.schema import SchemaService
from osa.domain.shared.authorization.gate import public
from osa.domain.shared.model.srn import SchemaSRN
from osa.domain.shared.query import Query, QueryHandler, Result


class ListSchemas(Query):
    pass


class SchemaSummary(BaseModel):
    srn: SchemaSRN
    title: str
    field_count: int
    created_at: datetime


class SchemaList(Result):
    items: list[SchemaSummary]


class ListSchemasHandler(QueryHandler[ListSchemas, SchemaList]):
    __auth__ = public()
    schema_service: SchemaService

    async def run(self, cmd: ListSchemas) -> SchemaList:
        schemas = await self.schema_service.list_schemas()
        return SchemaList(
            items=[
                SchemaSummary(
                    srn=s.srn,
                    title=s.title,
                    field_count=len(s.fields),
                    created_at=s.created_at,
                )
                for s in schemas
            ]
        )
