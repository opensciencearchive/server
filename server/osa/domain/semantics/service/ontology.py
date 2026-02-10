from datetime import UTC, datetime
from uuid import uuid4

from osa.domain.semantics.model.ontology import Ontology, Term
from osa.domain.semantics.port.ontology_repository import OntologyRepository
from osa.domain.semantics.util.obographs import parse_obographs
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.model.srn import Domain, LocalId, OntologySRN, Semver
from osa.domain.shared.service import Service


class OntologyService(Service):
    ontology_repo: OntologyRepository
    node_domain: Domain

    async def import_from_obographs(
        self,
        data: dict,
        *,
        version_override: str | None = None,
    ) -> Ontology:
        """Parse OBO Graphs JSON and create an ontology from it."""
        parsed = parse_obographs(data)
        version = version_override or parsed.version or "1.0.0"
        return await self.create_ontology(
            title=parsed.title,
            version=version,
            terms=parsed.terms,
            description=parsed.description,
        )

    async def create_ontology(
        self,
        title: str,
        version: str,
        terms: list[Term],
        description: str | None = None,
    ) -> Ontology:
        srn = OntologySRN(
            domain=self.node_domain,
            id=LocalId(str(uuid4())[:20]),
            version=Semver.from_string(version),
        )
        ontology = Ontology(
            srn=srn,
            title=title,
            description=description,
            terms=terms,
            created_at=datetime.now(UTC),
        )
        await self.ontology_repo.save(ontology)
        return ontology

    async def get_ontology(self, srn: OntologySRN) -> Ontology:
        ontology = await self.ontology_repo.get(srn)
        if ontology is None:
            raise NotFoundError(f"Ontology not found: {srn}")
        return ontology

    async def list_ontologies(
        self, *, limit: int | None = None, offset: int | None = None
    ) -> list[Ontology]:
        return await self.ontology_repo.list(limit=limit, offset=offset)
