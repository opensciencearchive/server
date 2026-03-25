"""GetRecord query handler — public read access to published records."""

from datetime import datetime
from typing import Any

from osa.domain.record.service.record import RecordService
from osa.domain.shared.authorization.gate import public
from osa.domain.shared.model.source import RecordSource
from osa.domain.shared.model.srn import ConventionSRN, RecordSRN
from osa.domain.shared.query import Query, QueryHandler, Result


class GetRecord(Query):
    srn: RecordSRN


class RecordDetail(Result):
    srn: RecordSRN
    source: RecordSource
    convention_srn: ConventionSRN
    metadata: dict[str, Any]
    published_at: datetime
    features: dict[str, list[dict[str, Any]]] = {}


class GetRecordHandler(QueryHandler[GetRecord, RecordDetail]):
    __auth__ = public()
    record_service: RecordService

    async def run(self, cmd: GetRecord) -> RecordDetail:
        record = await self.record_service.get(cmd.srn)
        features = await self.record_service.get_features_for_record(cmd.srn)
        return RecordDetail(
            srn=record.srn,
            source=record.source,
            convention_srn=record.convention_srn,
            metadata=record.metadata,
            published_at=record.published_at,
            features=features,
        )
