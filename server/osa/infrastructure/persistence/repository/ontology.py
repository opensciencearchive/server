from typing import Any, List
from uuid import uuid4

from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.semantics.model.ontology import Ontology, Term
from osa.domain.semantics.port.ontology_repository import OntologyRepository
from osa.domain.shared.model.srn import OntologySRN
from osa.infrastructure.persistence.tables import ontologies_table, ontology_terms_table


def _ontology_to_rows(ontology: Ontology) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Convert Ontology aggregate to table rows."""
    header = {
        "srn": str(ontology.srn),
        "title": ontology.title,
        "description": ontology.description,
        "created_at": ontology.created_at,
    }
    terms = [
        {
            "id": str(uuid4()),
            "ontology_srn": str(ontology.srn),
            "term_id": t.term_id,
            "label": t.label,
            "synonyms": t.synonyms,
            "parent_ids": t.parent_ids,
            "definition": t.definition,
            "deprecated": t.deprecated,
        }
        for t in ontology.terms
    ]
    return header, terms


def _rows_to_ontology(header: dict[str, Any], term_rows: list[dict[str, Any]]) -> Ontology:
    """Convert table rows back to Ontology aggregate."""
    terms = [
        Term(
            term_id=r["term_id"],
            label=r["label"],
            synonyms=r.get("synonyms", []),
            parent_ids=r.get("parent_ids", []),
            definition=r.get("definition"),
            deprecated=r.get("deprecated", False),
        )
        for r in term_rows
    ]
    return Ontology(
        srn=OntologySRN.parse(header["srn"]),
        title=header["title"],
        description=header.get("description"),
        terms=terms,
        created_at=header["created_at"],
    )


class PostgresOntologyRepository(OntologyRepository):
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def save(self, ontology: Ontology) -> None:
        header, terms = _ontology_to_rows(ontology)
        await self.session.execute(insert(ontologies_table).values(**header))
        if terms:
            await self.session.execute(insert(ontology_terms_table).values(terms))
        await self.session.flush()

    async def get(self, srn: OntologySRN) -> Ontology | None:
        stmt = select(ontologies_table).where(ontologies_table.c.srn == str(srn))
        result = await self.session.execute(stmt)
        header = result.mappings().first()
        if not header:
            return None

        terms_stmt = select(ontology_terms_table).where(
            ontology_terms_table.c.ontology_srn == str(srn)
        )
        terms_result = await self.session.execute(terms_stmt)
        term_rows = [dict(r) for r in terms_result.mappings().all()]

        return _rows_to_ontology(dict(header), term_rows)

    async def list(self, *, limit: int | None = None, offset: int | None = None) -> List[Ontology]:
        stmt = select(ontologies_table).order_by(ontologies_table.c.created_at.desc())
        if offset is not None:
            stmt = stmt.offset(offset)
        if limit is not None:
            stmt = stmt.limit(limit)

        result = await self.session.execute(stmt)
        headers = [dict(r) for r in result.mappings().all()]

        ontologies = []
        for header in headers:
            terms_stmt = select(ontology_terms_table).where(
                ontology_terms_table.c.ontology_srn == header["srn"]
            )
            terms_result = await self.session.execute(terms_stmt)
            term_rows = [dict(r) for r in terms_result.mappings().all()]
            ontologies.append(_rows_to_ontology(header, term_rows))

        return ontologies

    async def exists(self, srn: OntologySRN) -> bool:
        stmt = select(ontologies_table.c.srn).where(ontologies_table.c.srn == str(srn))
        result = await self.session.execute(stmt)
        return result.first() is not None
