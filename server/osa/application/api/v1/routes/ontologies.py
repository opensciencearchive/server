"""Ontology REST routes."""

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter

from osa.domain.semantics.command.create_ontology import (
    CreateOntology,
    CreateOntologyHandler,
    OntologyCreated,
)
from osa.domain.semantics.command.import_ontology import (
    ImportOntology,
    ImportOntologyHandler,
    ImportOntologyResult,
)
from osa.domain.semantics.query.get_ontology import (
    GetOntology,
    GetOntologyHandler,
    OntologyDetail,
)
from osa.domain.semantics.query.list_ontologies import (
    ListOntologies,
    ListOntologiesHandler,
    OntologyList,
)
from osa.domain.shared.model.srn import OntologySRN

router = APIRouter(prefix="/ontologies", tags=["Ontologies"], route_class=DishkaRoute)


@router.post("", response_model=OntologyCreated, status_code=201)
async def create_ontology(
    body: CreateOntology,
    handler: FromDishka[CreateOntologyHandler],
) -> OntologyCreated:
    return await handler.run(body)


@router.post("/import", response_model=ImportOntologyResult, status_code=201)
async def import_ontology(
    body: ImportOntology,
    handler: FromDishka[ImportOntologyHandler],
) -> ImportOntologyResult:
    return await handler.run(body)


@router.get("/{srn:path}", response_model=OntologyDetail)
async def get_ontology(
    srn: str,
    handler: FromDishka[GetOntologyHandler],
) -> OntologyDetail:
    return await handler.run(GetOntology(srn=OntologySRN.parse(srn)))


@router.get("", response_model=OntologyList)
async def list_ontologies(
    handler: FromDishka[ListOntologiesHandler],
) -> OntologyList:
    return await handler.run(ListOntologies())
