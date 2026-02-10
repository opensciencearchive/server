from typing import Any, List

from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.semantics.model.schema import Schema
from osa.domain.semantics.model.value import FieldDefinition
from osa.domain.semantics.port.schema_repository import SchemaRepository
from osa.domain.shared.model.srn import SchemaSRN
from osa.infrastructure.persistence.tables import schemas_table


def _schema_to_row(schema: Schema) -> dict[str, Any]:
    return {
        "srn": str(schema.srn),
        "title": schema.title,
        "fields": [f.model_dump(mode="json") for f in schema.fields],
        "created_at": schema.created_at,
    }


def _row_to_schema(row: dict[str, Any]) -> Schema:
    fields = [FieldDefinition.model_validate(f) for f in row["fields"]]
    return Schema(
        srn=SchemaSRN.parse(row["srn"]),
        title=row["title"],
        fields=fields,
        created_at=row["created_at"],
    )


class PostgresSemanticsSchemaRepository(SchemaRepository):
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def save(self, schema: Schema) -> None:
        row = _schema_to_row(schema)
        await self.session.execute(insert(schemas_table).values(**row))
        await self.session.flush()

    async def get(self, srn: SchemaSRN) -> Schema | None:
        stmt = select(schemas_table).where(schemas_table.c.srn == str(srn))
        result = await self.session.execute(stmt)
        row = result.mappings().first()
        return _row_to_schema(dict(row)) if row else None

    async def list(self, *, limit: int | None = None, offset: int | None = None) -> List[Schema]:
        stmt = select(schemas_table).order_by(schemas_table.c.created_at.desc())
        if offset is not None:
            stmt = stmt.offset(offset)
        if limit is not None:
            stmt = stmt.limit(limit)

        result = await self.session.execute(stmt)
        return [_row_to_schema(dict(r)) for r in result.mappings().all()]

    async def exists(self, srn: SchemaSRN) -> bool:
        stmt = select(schemas_table.c.srn).where(schemas_table.c.srn == str(srn))
        result = await self.session.execute(stmt)
        return result.first() is not None
