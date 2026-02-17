from datetime import datetime

from pydantic import BaseModel

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.deposition.model.value import DepositionStatus
from osa.domain.deposition.service.deposition import DepositionService
from osa.domain.shared.authorization.gate import at_least
from osa.domain.shared.model.srn import ConventionSRN, DepositionSRN
from osa.domain.shared.query import Query, QueryHandler, Result


class ListDepositions(Query):
    pass


class DepositionSummary(BaseModel):
    srn: DepositionSRN
    convention_srn: ConventionSRN
    status: DepositionStatus
    file_count: int
    created_at: datetime
    updated_at: datetime


class DepositionList(Result):
    items: list[DepositionSummary]
    total: int


class ListDepositionsHandler(QueryHandler[ListDepositions, DepositionList]):
    __auth__ = at_least(Role.DEPOSITOR)
    principal: Principal
    deposition_service: DepositionService

    async def run(self, cmd: ListDepositions) -> DepositionList:
        # Curators see all depositions; depositors see only their own
        owner_id = None if self.principal.has_role(Role.CURATOR) else self.principal.user_id
        depositions, total = await self.deposition_service.list_depositions(owner_id)
        return DepositionList(
            items=[
                DepositionSummary(
                    srn=d.srn,
                    convention_srn=d.convention_srn,
                    status=d.status,
                    file_count=len(d.files),
                    created_at=d.created_at,
                    updated_at=d.updated_at,
                )
                for d in depositions
            ],
            total=total,
        )
