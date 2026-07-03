from datetime import datetime

from osa.domain.deposition.model.docs import ConventionDocs
from osa.domain.deposition.model.value import FileRequirements
from osa.domain.deposition.service.convention import ConventionService
from osa.domain.shared.authorization.gate import public
from osa.domain.shared.model.hook import HookName
from osa.domain.shared.model.source import IngesterDefinition
from osa.domain.shared.model.srn import ConventionSlug, SchemaId
from osa.domain.shared.query import Query, QueryHandler, Result


class GetConvention(Query):
    id: ConventionSlug


class ConventionDetail(Result):
    id: ConventionSlug
    title: str
    description: str
    schema_id: SchemaId
    file_requirements: FileRequirements
    hooks: list[HookName]
    ingester: IngesterDefinition | None = None
    docs: ConventionDocs
    created_at: datetime


class GetConventionHandler(QueryHandler[GetConvention, ConventionDetail]):
    __auth__ = public()
    convention_service: ConventionService

    async def run(self, cmd: GetConvention) -> ConventionDetail:
        conv = await self.convention_service.get_convention(cmd.id)
        return ConventionDetail(
            id=conv.id,
            title=conv.title,
            description=conv.description,
            schema_id=conv.schema_id,
            file_requirements=conv.file_requirements,
            hooks=list(conv.hooks),
            ingester=conv.ingester,
            docs=conv.docs,
            created_at=conv.created_at,
        )
