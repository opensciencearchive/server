"""RunIngester — runs ingester container on NextBatchRequested."""

from datetime import UTC, datetime, timedelta
from typing import assert_never
from uuid import uuid4

from osa.domain.deposition.service.convention import ConventionService
from osa.domain.ingest.event.events import IngesterBatchReady, NextBatchRequested
from osa.domain.ingest.model.ingest_run import IngestStatus
from osa.domain.ingest.port.repository import IngestRunRepository
from osa.domain.ingest.port.storage import IngestStoragePort
from osa.domain.ingest.service.ingest import IngestService
from osa.domain.shared.error import NotFoundError, TransientError
from osa.domain.shared.event import EventHandler, EventId
from osa.domain.shared.failure import (
    AbortRun,
    FailurePolicy,
    GiveUp,
    PriorAttempts,
    Retry,
    RetryWithMoreMemory,
    RuntimeFailure,
)
from osa.domain.shared.model.srn import ConventionSlug
from osa.domain.shared.outbox import Outbox
from osa.domain.shared.port.ingester_runner import IngesterInputs, IngesterRunner
from osa.infrastructure.logging import get_logger

BACKPRESSURE_DELAY = timedelta(seconds=60)

log = get_logger(__name__)


class RunIngester(EventHandler[NextBatchRequested]):
    """Runs ingester container and emits IngesterBatchReady per batch."""

    __claim_timeout__ = 3600.0
    __max_retries__ = 20

    ingest_repo: IngestRunRepository
    ingest_service: IngestService
    convention_service: ConventionService
    ingester_runner: IngesterRunner
    outbox: Outbox
    ingest_storage: IngestStoragePort
    failure_policy: FailurePolicy

    async def handle(self, event: NextBatchRequested) -> None:
        ingest_run = await self.ingest_repo.get(event.ingest_run_id)
        if ingest_run is None:
            raise NotFoundError(f"Ingest run not found: {event.ingest_run_id}")

        # An aborted/finished run pulls no more batches — drop redelivered
        # events instead of sourcing work nobody will process (#152).
        if ingest_run.status in (IngestStatus.COMPLETED, IngestStatus.FAILED):
            log.warn(
                "next-batch request skipped — run is {status}",
                status=ingest_run.status.value,
                ingest_run_id=event.ingest_run_id,
            )
            return

        # Backpressure: don't ingest if the cluster can't schedule more Jobs
        if not await self.ingester_runner.has_capacity():
            log.info(
                "[{short_id}] backpressure: cluster has pending Jobs, deferring next pull +{delay}s",
                short_id=event.ingest_run_id[:8],
                delay=int(BACKPRESSURE_DELAY.total_seconds()),
                ingest_run_id=event.ingest_run_id,
            )
            await self.outbox.append(
                NextBatchRequested(
                    id=EventId(uuid4()),
                    ingest_run_id=event.ingest_run_id,
                    convention_id=event.convention_id,
                    batch_size=event.batch_size,
                    batch_index=event.batch_index,
                ),
                deliver_after=datetime.now(UTC) + BACKPRESSURE_DELAY,
            )
            return

        if ingest_run.status == IngestStatus.PENDING:
            ingest_run.mark_running()
            await self.ingest_repo.save(ingest_run)

        convention = await self.convention_service.get_convention(
            ConventionSlug.parse(event.convention_id)
        )
        if convention.ingester is None:
            raise NotFoundError(f"No ingester for convention {event.convention_id}")

        batch_index = event.batch_index

        session = await self.ingest_storage.read_session(event.ingest_run_id)

        effective_batch_limit = ingest_run.batch_size
        if ingest_run.limit is not None:
            ingested_so_far = ingest_run.batches_ingested * ingest_run.batch_size
            remaining = ingest_run.limit - ingested_so_far
            if remaining <= 0:
                log.warn(
                    "Ignoring redelivered NextBatchRequested — limit already met (batches_ingested={batches_ingested}, limit={limit})",
                    batches_ingested=ingest_run.batches_ingested,
                    limit=ingest_run.limit,
                    ingest_run_id=event.ingest_run_id,
                )
                await self.ingest_repo.increment_batches_ingested(
                    event.ingest_run_id,
                    set_ingestion_finished=True,
                )
                return
            effective_batch_limit = min(ingest_run.batch_size, remaining)

        inputs = IngesterInputs(
            convention_id=convention.id,
            ingest_run_id=event.ingest_run_id,
            batch_index=batch_index,
            config=convention.ingester.config,
            limit=effective_batch_limit,
            session=session,
        )
        work_dir = self.ingest_storage.batch_work_dir(event.ingest_run_id, batch_index)
        files_dir = self.ingest_storage.batch_files_dir(event.ingest_run_id, batch_index)

        try:
            output = await self.ingester_runner.run(
                ingester=convention.ingester,
                inputs=inputs,
                files_dir=files_dir,
                work_dir=work_dir,
            )
        except RuntimeFailure as failure:
            # The runner reports facts; the policy decides; we execute the verb.
            # Ingester Jobs have no memory-bump lever, so PriorAttempts carries
            # no bumps and an OOM decision degrades to a give-up below.
            decision = self.failure_policy.decide(failure, PriorAttempts(memory_bumps=0))
            match decision:
                case AbortRun(reason=reason, kind=kind):
                    log.error(
                        "[{short_id}] ingester failed for the whole run ({kind}): {error}",
                        short_id=event.ingest_run_id[:8],
                        kind=kind.value,
                        error=reason,
                        container_logs=failure.container_logs or "",
                        ingest_run_id=event.ingest_run_id,
                    )
                    await self.ingest_service.abort_run(
                        event.ingest_run_id, reason=reason, kind=kind
                    )
                case Retry():
                    # Re-raise for the worker's budgeted redelivery with backoff;
                    # exhaustion lands in on_exhausted → fail_ingestion.
                    raise TransientError(failure.detail) from failure
                case GiveUp(reason=reason, kind=kind):
                    log.error(
                        "[{short_id}] ingester gave up ({kind}): {error}",
                        short_id=event.ingest_run_id[:8],
                        kind=kind.value,
                        error=reason,
                        container_logs=failure.container_logs or "",
                        ingest_run_id=event.ingest_run_id,
                    )
                    await self.ingest_service.fail_ingestion(
                        event.ingest_run_id, reason=reason, kind=kind
                    )
                case RetryWithMoreMemory():
                    # Ingester Jobs have no memory-bump lever (unlike hooks, which
                    # retry inside HookService), so an OOM can't be remediated —
                    # degrade to a give-up carrying the original OOM facts.
                    log.error(
                        "[{short_id}] ingester OOM, no memory-bump lever: {error}",
                        short_id=event.ingest_run_id[:8],
                        error=failure.detail,
                        container_logs=failure.container_logs or "",
                        ingest_run_id=event.ingest_run_id,
                    )
                    await self.ingest_service.fail_ingestion(
                        event.ingest_run_id, reason=failure.detail, kind=failure.kind
                    )
                case _:
                    # Every Decision variant is handled above; a new one added to
                    # the union will fail `ty` here (and raise at runtime).
                    assert_never(decision)
            return

        await self.ingest_storage.write_records(event.ingest_run_id, batch_index, output.records)

        if output.session:
            await self.ingest_storage.write_session(event.ingest_run_id, output.session)

        has_more = output.session is not None and len(output.records) > 0

        if has_more and ingest_run.limit is not None:
            total_sourced = (ingest_run.batches_ingested + 1) * ingest_run.batch_size
            if total_sourced >= ingest_run.limit:
                has_more = False

        await self.ingest_repo.increment_batches_ingested(
            event.ingest_run_id,
            set_ingestion_finished=not has_more,
        )

        await self.outbox.append(
            IngesterBatchReady(
                id=EventId(uuid4()),
                ingest_run_id=event.ingest_run_id,
                batch_index=batch_index,
                has_more=has_more,
            )
        )

        short_id = event.ingest_run_id[:8]
        log.info(
            "[{short_id}] batch {batch_index}: pulled {record_count} records (has_more={has_more})",
            short_id=short_id,
            batch_index=batch_index,
            record_count=len(output.records),
            has_more=has_more,
            ingest_run_id=event.ingest_run_id,
        )

        if has_more:
            await self.outbox.append(
                NextBatchRequested(
                    id=EventId(uuid4()),
                    ingest_run_id=event.ingest_run_id,
                    convention_id=event.convention_id,
                    batch_size=ingest_run.batch_size,
                    batch_index=event.batch_index + 1,
                )
            )

    async def on_exhausted(self, event: NextBatchRequested) -> None:
        """Transient retries exhausted — stop ingestion and check completion."""
        log.error(
            "ingester retries exhausted",
            ingest_run_id=event.ingest_run_id,
        )
        await self.ingest_service.fail_ingestion(
            event.ingest_run_id, reason="ingester retries exhausted", kind=None
        )
