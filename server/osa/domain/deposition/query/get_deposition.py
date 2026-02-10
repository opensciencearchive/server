from datetime import datetime
from typing import Any

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.deposition.model.value import DepositionFile, DepositionStatus
from osa.domain.deposition.service.deposition import DepositionService
from osa.domain.shared.authorization.gate import at_least
from osa.domain.shared.model.srn import ConventionSRN, DepositionSRN, RecordSRN
from osa.domain.shared.query import Query, QueryHandler, Result


class GetDeposition(Query):
    srn: DepositionSRN


class DepositionDetail(Result):
    srn: DepositionSRN
    convention_srn: ConventionSRN
    status: DepositionStatus
    metadata: dict[str, Any]
    files: list[DepositionFile]
    record_srn: RecordSRN | None
    created_at: datetime
    updated_at: datetime


class GetDepositionHandler(QueryHandler[GetDeposition, DepositionDetail]):
    __auth__ = at_least(Role.DEPOSITOR)
    principal: Principal
    deposition_service: DepositionService

    async def run(self, cmd: GetDeposition) -> DepositionDetail:
        dep = await self.deposition_service.get(cmd.srn)
        return DepositionDetail(
            srn=dep.srn,
            convention_srn=dep.convention_srn,
            status=dep.status,
            metadata=dep.metadata,
            files=dep.files,
            record_srn=dep.record_srn,
            created_at=dep.created_at,
            updated_at=dep.updated_at,
        )
