from datetime import UTC, datetime
from uuid import uuid4

from osa.domain.semantics.model.schema import Schema
from osa.domain.semantics.model.value import FieldDefinition, FieldType, TermConstraints
from osa.domain.semantics.port.ontology_repository import OntologyRepository
from osa.domain.semantics.port.schema_repository import SchemaRepository
from osa.domain.shared.error import NotFoundError, ValidationError
from osa.domain.shared.model.srn import Domain, LocalId, SchemaSRN, Semver
from osa.domain.shared.service import Service


class SchemaService(Service):
    schema_repo: SchemaRepository
    ontology_repo: OntologyRepository
    node_domain: Domain

    async def create_schema(
        self,
        title: str,
        version: str,
        fields: list[FieldDefinition],
    ) -> Schema:
        # Validate ontology references
        for field in fields:
            if (
                field.type == FieldType.TERM
                and field.constraints is not None
                and isinstance(field.constraints, TermConstraints)
            ):
                exists = await self.ontology_repo.exists(field.constraints.ontology_srn)
                if not exists:
                    raise ValidationError(
                        f"Ontology '{field.constraints.ontology_srn}' not found "
                        f"(referenced by field '{field.name}')"
                    )

        srn = SchemaSRN(
            domain=self.node_domain,
            id=LocalId(str(uuid4())[:20]),
            version=Semver.from_string(version),
        )
        schema = Schema(
            srn=srn,
            title=title,
            fields=fields,
            created_at=datetime.now(UTC),
        )
        await self.schema_repo.save(schema)
        return schema

    async def get_schema(self, srn: SchemaSRN) -> Schema:
        schema = await self.schema_repo.get(srn)
        if schema is None:
            raise NotFoundError(f"Schema not found: {srn}")
        return schema

    async def list_schemas(
        self, *, limit: int | None = None, offset: int | None = None
    ) -> list[Schema]:
        return await self.schema_repo.list(limit=limit, offset=offset)
