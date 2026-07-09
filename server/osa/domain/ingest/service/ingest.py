"""IngestService — orchestrates ingest lifecycle."""

from datetime import UTC, datetime
from uuid import uuid4

from osa.domain.deposition.service.convention import ConventionService
from osa.domain.ingest.event.events import IngestCompleted, IngestRunStarted, NextBatchRequested
from osa.domain.ingest.model.ingest_run import (
    Applied,
    IngestRun,
    IngestRunId,
    IngestStatus,
    RunClosed,
)
from osa.domain.ingest.port.repository import IngestRunRepository
from osa.domain.shared.error import ConflictError, NotFoundError
from osa.domain.shared.event import EventId
from osa.domain.shared.failure import FailureKind
from osa.domain.shared.model.srn import ConventionSlug, Domain
from osa.domain.shared.outbox import Outbox
from osa.domain.shared.service import Service
from osa.infrastructure.logging import get_logger

log = get_logger(__name__)


class IngestService(Service):
    """Orchestrates ingest run creation and lifecycle."""

    ingest_repo: IngestRunRepository
    convention_service: ConventionService
    outbox: Outbox
    node_domain: Domain

    async def start_ingest(
        self,
        convention_id: str,  # TODO: use convention ID instead of SRN
        batch_size: int = 1000,
        limit: int | None = None,
    ) -> IngestRun:
        """Create an ingest run for a convention.

        Validates:
        - Convention exists
        - Convention has an ingester configured
        - No ingest is already running for this convention
        """
        parsed_srn = ConventionSlug.parse(convention_id)
        convention = await self.convention_service.get_convention(parsed_srn)

        if convention.ingester is None:
            raise NotFoundError(
                f"No ingester configured for convention {convention_id}",
                code="no_ingester_configured",
            )

        existing = await self.ingest_repo.get_running_for_convention(convention_id)
        if existing is not None:
            raise ConflictError(
                f"Ingest already running for convention {convention_id}",
                code="ingest_already_running",
            )

        run_id = IngestRunId(str(uuid4()))
        now = datetime.now(UTC)

        ingest_run = IngestRun(
            id=run_id,
            convention_id=convention_id,
            status=IngestStatus.PENDING,
            batch_size=batch_size,
            limit=limit,
            started_at=now,
        )

        await self.ingest_repo.save(ingest_run)

        await self.outbox.append(
            IngestRunStarted(
                id=EventId(uuid4()),
                ingest_run_id=run_id,
                convention_id=convention_id,
                batch_size=batch_size,
            )
        )

        await self.outbox.append(
            NextBatchRequested(
                id=EventId(uuid4()),
                ingest_run_id=run_id,
                convention_id=convention_id,
                batch_size=batch_size,
            )
        )

        srn = f"urn:osa:{self.node_domain.root}:ing:{run_id}"
        log.info(
            "ingest started for {convention_id}",
            ingest_run_srn=srn,
            convention_id=convention_id,
            batch_size=batch_size,
            limit=limit,
        )
        return ingest_run

    async def get_ingestion(self, ingest_run_id: IngestRunId) -> IngestRun:
        """Fetch an ingest run by id, raising NotFoundError if absent."""
        ingest_run = await self.ingest_repo.get(ingest_run_id)
        if ingest_run is None:
            raise NotFoundError(f"Ingest run not found: {ingest_run_id}")
        return ingest_run

    async def complete_batch(self, ingest_run_id: IngestRunId, published_count: int) -> None:
        """Account for a successfully processed batch.

        Increments batches_completed and published_count atomically,
        then checks the completion condition.
        """
        match await self.ingest_repo.increment_completed(
            ingest_run_id, published_count=published_count
        ):
            case RunClosed():
                # A late/stale batch reached a closed run — expected under
                # concurrency, but logged so it's never silent when we're
                # chasing "why is a counter off / a record missing".
                log.warn(
                    "complete_batch no-op — run already terminal",
                    ingest_run_id=ingest_run_id,
                    published_count=published_count,
                )
            case Applied(run):
                await self._check_completion(run)

    async def fail_batch(
        self, ingest_run_id: IngestRunId, *, reason: str, kind: FailureKind | None = None
    ) -> None:
        """Account for a batch that permanently failed hook/publish processing (#152).

        Increments batches_failed, completes the run if all batches are now
        accounted for (completed + failed >= ingested), and records why so an
        operator sees the explanation on the run. ``record_failure`` runs LAST:
        a completing run's aggregate save (in ``_check_completion``) would
        otherwise clobber the reason with the aggregate's stale null.
        """
        match await self.ingest_repo.increment_failed(ingest_run_id):
            case RunClosed():
                log.warn(
                    "fail_batch no-op — run already terminal",
                    ingest_run_id=ingest_run_id,
                    reason=reason,
                )
            case Applied(run):
                await self._check_completion(run)
                await self.ingest_repo.record_failure(ingest_run_id, reason=reason, kind=kind)

    async def fail_ingestion(
        self, ingest_run_id: IngestRunId, *, reason: str, kind: FailureKind | None
    ) -> None:
        """Account for a failed ingester pull, recording why (#152).

        The batch was never sourced, so we mark ingestion as finished
        (no more batches coming) and increment batches_failed. The completion
        condition can then fire based on whatever batches were already sourced.
        The reason/kind land on the run so an operator sees the explanation
        without log archaeology. ``record_failure`` runs LAST for the same
        reason as ``fail_batch`` — so a completing run's save can't clobber it.
        """
        match await self.ingest_repo.increment_batches_ingested(
            ingest_run_id,
            set_ingestion_finished=True,
        ):
            case RunClosed():
                log.warn(
                    "fail_ingestion no-op — run already terminal",
                    ingest_run_id=ingest_run_id,
                    reason=reason,
                )
                return
            case Applied():
                pass
        match await self.ingest_repo.increment_failed(ingest_run_id):
            case RunClosed():
                log.warn(
                    "fail_ingestion no-op — run terminalized concurrently",
                    ingest_run_id=ingest_run_id,
                    reason=reason,
                )
            case Applied(run):
                await self._check_completion(run)
                await self.ingest_repo.record_failure(ingest_run_id, reason=reason, kind=kind)

    async def abort_run(
        self, ingest_run_id: IngestRunId, *, reason: str, kind: FailureKind
    ) -> None:
        """Hard-stop a run on a deterministic environmental failure (#152).

        The failure would recur identically for every batch (unpullable image,
        RBAC, bad config), so the run is failed immediately with a queryable
        explanation and no further batches are scheduled. Idempotent: a run
        that is already terminal is left untouched (concurrent batch workers
        may race to abort).
        """
        match await self.ingest_repo.abort(
            ingest_run_id,
            reason=reason,
            kind=kind,
            completed_at=datetime.now(UTC),
        ):
            case RunClosed():
                log.warn(
                    "abort no-op — run already terminal",
                    ingest_run_id=ingest_run_id,
                )
            case Applied():
                log.error(
                    "[{short_id}] run ABORTED ({kind}): {reason}",
                    short_id=str(ingest_run_id)[:8],
                    kind=kind.value,
                    reason=reason,
                    ingest_run_id=ingest_run_id,
                )

    async def _check_completion(self, ingest_run: IngestRun) -> None:
        """Transition to COMPLETED and emit IngestCompleted if all batches are accounted for."""
        if not ingest_run.check_completion(datetime.now(UTC)):
            return
        await self.ingest_repo.save(ingest_run)
        await self.outbox.append(
            IngestCompleted(
                id=EventId(uuid4()),
                ingest_run_id=ingest_run.id,
                total_published=ingest_run.published_count,
            )
        )
        log.info(
            "[{short_id}] COMPLETE: {total_published} records published",
            short_id=str(ingest_run.id)[:8],
            total_published=ingest_run.published_count,
            ingest_run_id=ingest_run.id,
        )
