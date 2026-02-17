"""Import an ontology from an OBO Graphs JSON URL."""

from datetime import datetime

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.semantics.port.ontology_fetcher import OntologyFetcher
from osa.domain.semantics.service.ontology import OntologyService
from osa.domain.shared.authorization.gate import at_least
from osa.domain.shared.command import Command, CommandHandler, Result
from osa.domain.shared.model.srn import OntologySRN


class ImportOntology(Command):
    url: str
    version: str | None = None


class ImportOntologyResult(Result):
    srn: OntologySRN
    title: str
    description: str | None
    term_count: int
    created_at: datetime


class ImportOntologyHandler(CommandHandler[ImportOntology, ImportOntologyResult]):
    __auth__ = at_least(Role.ADMIN)
    principal: Principal
    ontology_service: OntologyService
    fetcher: OntologyFetcher

    async def run(self, cmd: ImportOntology) -> ImportOntologyResult:
        data = await self.fetcher.fetch_json(cmd.url)
        ontology = await self.ontology_service.import_from_obographs(
            data, version_override=cmd.version
        )
        return ImportOntologyResult(
            srn=ontology.srn,
            title=ontology.title,
            description=ontology.description,
            term_count=len(ontology.terms),
            created_at=ontology.created_at,
        )
