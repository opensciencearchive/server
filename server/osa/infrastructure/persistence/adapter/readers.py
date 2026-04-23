"""Cross-domain reader adapters.

These implement the deposition domain's read-only ports by querying the
semantics tables directly — no semantics domain code is imported for business logic.
"""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.deposition.port.ontology_reader import OntologyReader
from osa.domain.deposition.port.schema_reader import SchemaReader
from osa.domain.semantics.model.ontology import Ontology, Term
from osa.domain.semantics.model.schema import Schema
from osa.domain.semantics.model.value import FieldDefinition
from osa.domain.shared.model.srn import LocalId, OntologySRN, SchemaId, Semver
from osa.infrastructure.persistence.tables import (
    ontologies_table,
    ontology_terms_table,
    schemas_table,
)


def _where_schema(schema_id: SchemaId):
    return (schemas_table.c.id == schema_id.id.root) & (
        schemas_table.c.version == schema_id.version.root
    )


class SchemaReaderAdapter(SchemaReader):
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_schema(self, schema_id: SchemaId) -> Schema | None:
        stmt = select(schemas_table).where(_where_schema(schema_id))
        result = await self.session.execute(stmt)
        row = result.mappings().first()
        if not row:
            return None
        row_dict = dict(row)
        fields = [FieldDefinition.model_validate(f) for f in row_dict["fields"]]
        return Schema(
            id=SchemaId(
                id=LocalId(row_dict["id"]),
                version=Semver.from_string(row_dict["version"]),
            ),
            title=row_dict["title"],
            fields=fields,
            created_at=row_dict["created_at"],
        )

    async def schema_exists(self, schema_id: SchemaId) -> bool:
        stmt = select(schemas_table.c.id).where(_where_schema(schema_id))
        result = await self.session.execute(stmt)
        return result.first() is not None


class OntologyReaderAdapter(OntologyReader):
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_ontology(self, srn: OntologySRN) -> Ontology | None:
        stmt = select(ontologies_table).where(ontologies_table.c.srn == str(srn))
        result = await self.session.execute(stmt)
        header = result.mappings().first()
        if not header:
            return None

        header_dict = dict(header)
        terms_stmt = select(ontology_terms_table).where(
            ontology_terms_table.c.ontology_srn == str(srn)
        )
        terms_result = await self.session.execute(terms_stmt)
        term_rows = terms_result.mappings().all()

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
            srn=OntologySRN.parse(header_dict["srn"]),
            title=header_dict["title"],
            description=header_dict.get("description"),
            terms=terms,
            created_at=header_dict["created_at"],
        )
