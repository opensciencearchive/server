"""GetIngestion query — inspect an ingest run, including why it failed (#152)."""

from datetime import datetime

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.ingest.model.ingest_run import IngestRunId, IngestStatus
from osa.domain.ingest.service.ingest import IngestService
from osa.domain.shared.authorization.gate import at_least
from osa.domain.shared.failure import FailureKind
from osa.domain.shared.query import Query, QueryHandler, Result


class GetIngestion(Query):
    ingest_run_id: IngestRunId


class IngestRunDetail(Result):
    """Read shape of an ingest run.

    ``failure_reason``/``failure_kind`` carry the queryable explanation for a
    failed run (or an ingestion that stopped early) — the operator-facing
    surface for what previously survived only in delivery errors and app logs.
    """

    id: IngestRunId
    convention_id: str
    status: IngestStatus
    ingestion_finished: bool
    batches_ingested: int
    batches_completed: int
    batches_failed: int
    published_count: int
    batch_size: int
    limit: int | None
    started_at: datetime
    completed_at: datetime | None
    failure_reason: str | None
    failure_kind: FailureKind | None


class GetIngestionHandler(QueryHandler[GetIngestion, IngestRunDetail]):
    """Thin query handler — delegates to IngestService."""

    __auth__ = at_least(Role.ADMIN)

    principal: Principal
    service: IngestService

    async def run(self, cmd: GetIngestion) -> IngestRunDetail:
        ingest_run = await self.service.get_ingestion(cmd.ingest_run_id)
        return IngestRunDetail(
            id=ingest_run.id,
            convention_id=ingest_run.convention_id,
            status=ingest_run.status,
            ingestion_finished=ingest_run.ingestion_finished,
            batches_ingested=ingest_run.batches_ingested,
            batches_completed=ingest_run.batches_completed,
            batches_failed=ingest_run.batches_failed,
            published_count=ingest_run.published_count,
            batch_size=ingest_run.batch_size,
            limit=ingest_run.limit,
            started_at=ingest_run.started_at,
            completed_at=ingest_run.completed_at,
            failure_reason=ingest_run.failure_reason,
            failure_kind=ingest_run.failure_kind,
        )
