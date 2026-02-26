"""GetRecord query handler — public read access to published records."""

from datetime import datetime
from typing import Any

from osa.domain.record.service.record import RecordService
from osa.domain.shared.authorization.gate import public
from osa.domain.shared.model.srn import DepositionSRN, RecordSRN
from osa.domain.shared.query import Query, QueryHandler, Result


class GetRecord(Query):
    srn: RecordSRN


class RecordDetail(Result):
    srn: RecordSRN
    deposition_srn: DepositionSRN
    metadata: dict[str, Any]
    published_at: datetime


class GetRecordHandler(QueryHandler[GetRecord, RecordDetail]):
    __auth__ = public()
    record_service: RecordService

    async def run(self, cmd: GetRecord) -> RecordDetail:
        record = await self.record_service.get(cmd.srn)
        return RecordDetail(
            srn=record.srn,
            deposition_srn=record.deposition_srn,
            metadata=record.metadata,
            published_at=record.published_at,
        )
