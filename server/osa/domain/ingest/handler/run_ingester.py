"""RunIngester — runs ingester container on NextBatchRequested."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

from osa.domain.deposition.service.convention import ConventionService
from osa.domain.ingest.event.events import IngesterBatchReady, NextBatchRequested
from osa.domain.ingest.model.ingest_run import IngestStatus
from osa.domain.ingest.port.repository import IngestRunRepository
from osa.domain.ingest.port.storage import IngestStoragePort
from osa.domain.ingest.service.ingest import IngestService
from osa.domain.shared.error import NotFoundError, PermanentError
from osa.domain.shared.event import EventHandler, EventId
from osa.domain.shared.model.srn import ConventionSRN
from osa.domain.shared.outbox import Outbox
from osa.domain.shared.port.ingester_runner import IngesterInputs, IngesterRunner
from osa.infrastructure.logging import get_logger

BACKPRESSURE_DELAY = timedelta(seconds=60)
MAX_PENDING_BATCHES = 4

log = get_logger(__name__)


class RunIngester(EventHandler[NextBatchRequested]):
    """Runs ingester container and emits IngesterBatchReady per batch."""

    __claim_timeout__ = 3600.0

    ingest_repo: IngestRunRepository
    ingest_service: IngestService
    convention_service: ConventionService
    ingester_runner: IngesterRunner
    outbox: Outbox
    ingest_storage: IngestStoragePort

    async def handle(self, event: NextBatchRequested) -> None:
        ingest_run = await self.ingest_repo.get(event.ingest_run_id)
        if ingest_run is None:
            raise NotFoundError(f"Ingest run not found: {event.ingest_run_id}")

        # Backpressure: don't ingest faster than hooks can process
        pending = (
            ingest_run.batches_ingested - ingest_run.batches_completed - ingest_run.batches_failed
        )
        if pending >= MAX_PENDING_BATCHES:
            log.info(
                "[{short_id}] backpressure: {pending} batches pending, deferring next pull",
                short_id=event.ingest_run_id[:8],
                pending=pending,
                ingest_run_id=event.ingest_run_id,
            )
            await self.outbox.append(
                NextBatchRequested(
                    id=EventId(uuid4()),
                    ingest_run_id=event.ingest_run_id,
                    convention_srn=event.convention_srn,
                    batch_size=event.batch_size,
                ),
                deliver_after=datetime.now(UTC) + BACKPRESSURE_DELAY,
            )
            return

        if ingest_run.status == IngestStatus.PENDING:
            ingest_run.mark_running()
            await self.ingest_repo.save(ingest_run)

        convention = await self.convention_service.get_convention(
            ConventionSRN.parse(event.convention_srn)
        )
        if convention.ingester is None:
            raise NotFoundError(f"No ingester for convention {event.convention_srn}")

        batch_index = ingest_run.batches_ingested

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
            convention_srn=convention.srn,
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
        except PermanentError as e:
            container_logs = await self.ingester_runner.capture_logs(event.ingest_run_id)
            log.error(
                "[{short_id}] ingester permanently failed: {error}",
                short_id=event.ingest_run_id[:8],
                error=str(e),
                container_logs=container_logs,
                ingest_run_id=event.ingest_run_id,
            )
            await self._fail_ingestion(event)
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
                    convention_srn=event.convention_srn,
                    batch_size=ingest_run.batch_size,
                )
            )

    async def on_exhausted(self, event: NextBatchRequested) -> None:
        """Transient retries exhausted — stop ingestion and check completion."""
        log.error(
            "ingester retries exhausted",
            ingest_run_id=event.ingest_run_id,
        )
        await self._fail_ingestion(event)

    async def _fail_ingestion(self, event: NextBatchRequested) -> None:
        """Account for a permanently failed ingester pull."""
        await self.ingest_service.fail_ingestion(event.ingest_run_id)
