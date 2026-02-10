from datetime import datetime

from pydantic import BaseModel

from osa.domain.semantics.service.ontology import OntologyService
from osa.domain.shared.authorization.gate import public
from osa.domain.shared.model.srn import OntologySRN
from osa.domain.shared.query import Query, QueryHandler, Result


class ListOntologies(Query):
    pass


class OntologySummary(BaseModel):
    srn: OntologySRN
    title: str
    description: str | None
    term_count: int
    created_at: datetime


class OntologyList(Result):
    items: list[OntologySummary]


class ListOntologiesHandler(QueryHandler[ListOntologies, OntologyList]):
    __auth__ = public()
    ontology_service: OntologyService

    async def run(self, cmd: ListOntologies) -> OntologyList:
        ontologies = await self.ontology_service.list_ontologies()
        return OntologyList(
            items=[
                OntologySummary(
                    srn=o.srn,
                    title=o.title,
                    description=o.description,
                    term_count=len(o.terms),
                    created_at=o.created_at,
                )
                for o in ontologies
            ]
        )
