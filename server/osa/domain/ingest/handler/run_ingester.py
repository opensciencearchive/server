"""RunIngester — runs ingester container on IngestStarted or continuation."""

import json
from uuid import uuid4

from osa.domain.deposition.service.convention import ConventionService
from osa.domain.ingest.event.events import IngestStarted, IngesterBatchReady
from osa.domain.ingest.model.ingest_run import IngestStatus
from osa.domain.ingest.port.repository import IngestRunRepository
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.event import EventHandler, EventId
from osa.domain.shared.model.srn import ConventionSRN
from osa.domain.shared.outbox import Outbox
from osa.domain.shared.port.ingester_runner import IngesterInputs, IngesterRunner
from osa.infrastructure.logging import get_logger
from osa.infrastructure.storage.layout import StorageLayout

log = get_logger(__name__)


class RunIngester(EventHandler[IngestStarted]):
    """Runs ingester container and emits IngesterBatchReady per batch."""

    __claim_timeout__ = 3600.0

    ingest_repo: IngestRunRepository
    convention_service: ConventionService
    ingester_runner: IngesterRunner
    outbox: Outbox
    layout: StorageLayout

    async def handle(self, event: IngestStarted) -> None:
        """Run ingester for the given ingest run and emit IngesterBatchReady.

        TODO: move this log into a service method.
        """
        ingest_run = await self.ingest_repo.get(event.ingest_run_srn)
        if ingest_run is None:
            raise NotFoundError(f"Ingest run not found: {event.ingest_run_srn}")

        if ingest_run.status == IngestStatus.PENDING:
            ingest_run.mark_running()
            await self.ingest_repo.save(ingest_run)

        convention = await self.convention_service.get_convention(
            ConventionSRN.parse(event.convention_srn)
        )
        if convention.ingester is None:
            raise NotFoundError(f"No ingester for convention {event.convention_srn}")

        batch_index = ingest_run.batches_ingested

        batch_dir = self.layout.ingest_batch_ingester_dir(event.ingest_run_srn, batch_index)
        batch_dir.mkdir(parents=True, exist_ok=True)

        session_file = self.layout.ingest_session_file(event.ingest_run_srn)
        session = None
        if session_file.exists():
            session = json.loads(session_file.read_text())

        effective_batch_limit = ingest_run.batch_size
        if ingest_run.limit is not None:
            ingested_so_far = ingest_run.batches_ingested * ingest_run.batch_size
            remaining = ingest_run.limit - ingested_so_far
            if remaining <= 0:
                await self.ingest_repo.increment_batches_ingested(
                    event.ingest_run_srn, set_ingestion_finished=True
                )
                return
            effective_batch_limit = min(ingest_run.batch_size, remaining)

        inputs = IngesterInputs(
            convention_srn=convention.srn,
            config=convention.ingester.config,
            limit=effective_batch_limit,
            session=session,
        )
        files_dir = batch_dir / "files"
        files_dir.mkdir(parents=True, exist_ok=True)

        output = await self.ingester_runner.run(
            ingester=convention.ingester,
            inputs=inputs,
            files_dir=files_dir,
            work_dir=batch_dir,
        )

        records_file = batch_dir / "records.jsonl"
        with records_file.open("w") as f:
            for record in output.records:
                f.write(json.dumps(record) + "\n")

        if output.session:
            session_file.parent.mkdir(parents=True, exist_ok=True)
            session_file.write_text(json.dumps(output.session))

        has_more = output.session is not None and len(output.records) > 0

        if has_more and ingest_run.limit is not None:
            total_sourced = (ingest_run.batches_ingested + 1) * ingest_run.batch_size
            if total_sourced >= ingest_run.limit:
                has_more = False

        await self.ingest_repo.increment_batches_ingested(
            event.ingest_run_srn,
            set_ingestion_finished=not has_more,
        )

        await self.outbox.append(
            IngesterBatchReady(
                id=EventId(uuid4()),
                ingest_run_srn=event.ingest_run_srn,
                batch_index=batch_index,
                has_more=has_more,
            )
        )

        short_id = event.ingest_run_srn.rsplit(":", 1)[-1][:8]
        log.info(
            "[{short_id}] batch {batch_index}: pulled {record_count} records (has_more={has_more})",
            short_id=short_id,
            batch_index=batch_index,
            record_count=len(output.records),
            has_more=has_more,
            ingest_run_srn=event.ingest_run_srn,
        )

        if has_more:
            await self.outbox.append(
                IngestStarted(
                    id=EventId(uuid4()),
                    ingest_run_srn=event.ingest_run_srn,
                    convention_srn=event.convention_srn,
                    batch_size=ingest_run.batch_size,
                )
            )
