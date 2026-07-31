"""ListIngestions query — recent ingest runs, including in-progress ones."""

from datetime import datetime

from osa.domain.auth.model.principal import Principal
from osa.domain.ingest.model.ingest_run import IngestRunId, IngestStatus
from osa.domain.ingest.service.ingest import IngestService
from osa.domain.shared.authorization.gate import requires_scope
from osa.domain.shared.failure import FailureKind
from osa.domain.shared.query import Query, QueryHandler, Result


class ListIngestions(Query):
    pass


class IngestRunSummary(Result):
    """One ingest run in the list. Pending/running are still in-progress."""

    id: IngestRunId
    convention_id: str
    status: IngestStatus
    ingestion_finished: bool
    batches_ingested: int
    batches_completed: int
    batches_failed: int
    published_count: int
    started_at: datetime
    completed_at: datetime | None
    failure_reason: str | None
    failure_kind: FailureKind | None


class IngestRunList(Result):
    items: list[IngestRunSummary]


class ListIngestionsHandler(QueryHandler[ListIngestions, IngestRunList]):
    # Scoped M2M read (#184): a token with `ingestions:read` reads this surface;
    # ADMIN still passes (RequiresScope authorizes on scope OR ADMIN).
    __auth__ = requires_scope("ingestions:read")

    principal: Principal
    service: IngestService

    async def run(self, cmd: ListIngestions) -> IngestRunList:
        runs = await self.service.list_ingestions()
        return IngestRunList(
            items=[
                IngestRunSummary(
                    id=r.id,
                    convention_id=r.convention_id,
                    status=r.status,
                    ingestion_finished=r.ingestion_finished,
                    batches_ingested=r.batches_ingested,
                    batches_completed=r.batches_completed,
                    batches_failed=r.batches_failed,
                    published_count=r.published_count,
                    started_at=r.started_at,
                    completed_at=r.completed_at,
                    failure_reason=r.failure_reason,
                    failure_kind=r.failure_kind,
                )
                for r in runs
            ]
        )
