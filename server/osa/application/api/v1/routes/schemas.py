"""Schema REST routes."""

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter

from osa.domain.semantics.command.create_schema import (
    CreateSchema,
    CreateSchemaHandler,
    SchemaCreated,
)
from osa.domain.semantics.query.get_schema import (
    GetSchema,
    GetSchemaHandler,
    SchemaDetail,
)
from osa.domain.semantics.query.list_schemas import (
    ListSchemas,
    ListSchemasHandler,
    SchemaList,
)
from osa.domain.shared.error import ValidationError
from osa.domain.shared.model.srn import SchemaId

router = APIRouter(prefix="/schemas", tags=["Schemas"], route_class=DishkaRoute)


@router.post("", response_model=SchemaCreated, status_code=201)
async def create_schema(
    body: CreateSchema,
    handler: FromDishka[CreateSchemaHandler],
) -> SchemaCreated:
    return await handler.run(body)


@router.get("/{schema:path}", response_model=SchemaDetail)
async def get_schema(
    schema: str,
    handler: FromDishka[GetSchemaHandler],
) -> SchemaDetail:
    """Fetch a schema by its short id+version, e.g. ``"pdb-structure@1.0.0"``."""
    try:
        sid = SchemaId.parse(schema)
    except ValueError as exc:
        raise ValidationError(str(exc), field="schema") from exc
    return await handler.run(GetSchema(schema_id=sid))


@router.get("", response_model=SchemaList)
async def list_schemas(
    handler: FromDishka[ListSchemasHandler],
) -> SchemaList:
    return await handler.run(ListSchemas())
