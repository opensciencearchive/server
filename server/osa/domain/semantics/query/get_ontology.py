from datetime import datetime

from osa.domain.semantics.model.ontology import Term
from osa.domain.semantics.service.ontology import OntologyService
from osa.domain.shared.authorization.gate import public
from osa.domain.shared.model.srn import OntologySRN
from osa.domain.shared.query import Query, QueryHandler, Result


class GetOntology(Query):
    srn: OntologySRN


class OntologyDetail(Result):
    srn: OntologySRN
    title: str
    description: str | None
    terms: list[Term]
    created_at: datetime


class GetOntologyHandler(QueryHandler[GetOntology, OntologyDetail]):
    __auth__ = public()
    ontology_service: OntologyService

    async def run(self, cmd: GetOntology) -> OntologyDetail:
        ontology = await self.ontology_service.get_ontology(cmd.srn)
        return OntologyDetail(
            srn=ontology.srn,
            title=ontology.title,
            description=ontology.description,
            terms=ontology.terms,
            created_at=ontology.created_at,
        )
