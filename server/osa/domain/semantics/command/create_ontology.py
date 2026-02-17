from datetime import datetime

from pydantic import BaseModel

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.semantics.model.ontology import Term
from osa.domain.semantics.service.ontology import OntologyService
from osa.domain.shared.authorization.gate import at_least
from osa.domain.shared.command import Command, CommandHandler, Result
from osa.domain.shared.model.srn import OntologySRN


class TermInput(BaseModel):
    term_id: str
    label: str
    synonyms: list[str] = []
    parent_ids: list[str] = []
    definition: str | None = None
    deprecated: bool = False


class CreateOntology(Command):
    title: str
    version: str
    terms: list[TermInput]
    description: str | None = None


class OntologyCreated(Result):
    srn: OntologySRN
    title: str
    description: str | None
    term_count: int
    created_at: datetime


class CreateOntologyHandler(CommandHandler[CreateOntology, OntologyCreated]):
    __auth__ = at_least(Role.ADMIN)
    principal: Principal
    ontology_service: OntologyService

    async def run(self, cmd: CreateOntology) -> OntologyCreated:
        terms = [
            Term(
                term_id=t.term_id,
                label=t.label,
                synonyms=t.synonyms,
                parent_ids=t.parent_ids,
                definition=t.definition,
                deprecated=t.deprecated,
            )
            for t in cmd.terms
        ]
        ontology = await self.ontology_service.create_ontology(
            title=cmd.title,
            version=cmd.version,
            terms=terms,
            description=cmd.description,
        )
        return OntologyCreated(
            srn=ontology.srn,
            title=ontology.title,
            description=ontology.description,
            term_count=len(ontology.terms),
            created_at=ontology.created_at,
        )
