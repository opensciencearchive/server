"""Convention REST routes."""

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter

from osa.domain.deposition.command.create_convention import (
    CreateConvention,
    CreateConventionHandler,
    ConventionCreated,
)
from osa.domain.deposition.query.get_convention import (
    GetConvention,
    GetConventionHandler,
    ConventionDetail,
)
from osa.domain.deposition.query.list_conventions import (
    ListConventions,
    ListConventionsHandler,
    ConventionList,
)
from osa.domain.shared.model.srn import ConventionSRN

router = APIRouter(prefix="/conventions", tags=["Conventions"], route_class=DishkaRoute)


@router.post("", response_model=ConventionCreated, status_code=201)
async def create_convention(
    body: CreateConvention,
    handler: FromDishka[CreateConventionHandler],
) -> ConventionCreated:
    return await handler.run(body)


@router.get("/{srn:path}", response_model=ConventionDetail)
async def get_convention(
    srn: str,
    handler: FromDishka[GetConventionHandler],
) -> ConventionDetail:
    return await handler.run(GetConvention(srn=ConventionSRN.parse(srn)))


@router.get("", response_model=ConventionList)
async def list_conventions(
    handler: FromDishka[ListConventionsHandler],
) -> ConventionList:
    return await handler.run(ListConventions())
