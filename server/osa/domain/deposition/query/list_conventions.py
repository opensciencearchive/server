from datetime import datetime

from pydantic import BaseModel

from osa.domain.deposition.service.convention import ConventionService
from osa.domain.shared.authorization.gate import public
from osa.domain.shared.model.srn import ConventionSRN, SchemaSRN
from osa.domain.shared.query import Query, QueryHandler, Result


class ListConventions(Query):
    pass


class ConventionSummary(BaseModel):
    srn: ConventionSRN
    title: str
    description: str | None
    schema_srn: SchemaSRN
    created_at: datetime


class ConventionList(Result):
    items: list[ConventionSummary]


class ListConventionsHandler(QueryHandler[ListConventions, ConventionList]):
    __auth__ = public()
    convention_service: ConventionService

    async def run(self, cmd: ListConventions) -> ConventionList:
        conventions = await self.convention_service.list_conventions()
        return ConventionList(
            items=[
                ConventionSummary(
                    srn=c.srn,
                    title=c.title,
                    description=c.description,
                    schema_srn=c.schema_srn,
                    created_at=c.created_at,
                )
                for c in conventions
            ]
        )
