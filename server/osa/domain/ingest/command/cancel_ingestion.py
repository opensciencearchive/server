"""CancelIngestion command — stop a running ingest run (#227).

The operator's lever. Before this, ``/ingestions/{id}`` exposed only GET, so a
runaway run could be stopped only by stopping the server.
"""

from osa.domain.auth.model.principal import Principal
from osa.domain.ingest.model.ingest_run import IngestRunId, IngestStatus
from osa.domain.ingest.service.ingest import IngestService
from osa.domain.shared.authorization.gate import requires_scope
from osa.domain.shared.command import Command, CommandHandler, Result

_OPERATOR_CANCELLED = "cancelled by operator"


class CancelIngestion(Command):
    """Cancel an ingest run, stopping any further batch scheduling."""

    ingest_run_id: IngestRunId
    reason: str = _OPERATOR_CANCELLED


class IngestRunCancelled(Result):
    """The run's terminal state after cancelling."""

    id: IngestRunId
    convention_id: str
    status: IngestStatus
    batches_completed: int
    records_sourced: int
    published_count: int
    reason: str


class CancelIngestionHandler(CommandHandler[CancelIngestion, IngestRunCancelled]):
    """Thin command handler — delegates to IngestService."""

    # Stopping a run is the same blast radius as starting one, so it shares the
    # ``ingestions:write`` scope (OR ADMIN).
    __auth__ = requires_scope("ingestions:write")

    principal: Principal
    service: IngestService

    async def run(self, cmd: CancelIngestion) -> IngestRunCancelled:
        run = await self.service.cancel_ingestion(cmd.ingest_run_id, reason=cmd.reason)
        return IngestRunCancelled(
            id=run.id,
            convention_id=run.convention_id,
            status=run.status,
            batches_completed=run.batches_completed,
            records_sourced=run.records_sourced,
            published_count=run.published_count,
            reason=cmd.reason,
        )
