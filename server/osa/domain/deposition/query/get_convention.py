from datetime import datetime

from osa.domain.deposition.model.value import FileRequirements
from osa.domain.deposition.service.convention import ConventionService
from osa.domain.shared.authorization.gate import public
from osa.domain.shared.model.srn import ConventionSRN, SchemaSRN
from osa.domain.shared.model.validator import ValidatorRef
from osa.domain.shared.query import Query, QueryHandler, Result


class GetConvention(Query):
    srn: ConventionSRN


class ConventionDetail(Result):
    srn: ConventionSRN
    title: str
    description: str | None
    schema_srn: SchemaSRN
    file_requirements: FileRequirements
    validator_refs: list[ValidatorRef]
    created_at: datetime


class GetConventionHandler(QueryHandler[GetConvention, ConventionDetail]):
    __auth__ = public()
    convention_service: ConventionService

    async def run(self, cmd: GetConvention) -> ConventionDetail:
        conv = await self.convention_service.get_convention(cmd.srn)
        return ConventionDetail(
            srn=conv.srn,
            title=conv.title,
            description=conv.description,
            schema_srn=conv.schema_srn,
            file_requirements=conv.file_requirements,
            validator_refs=conv.validator_refs,
            created_at=conv.created_at,
        )
