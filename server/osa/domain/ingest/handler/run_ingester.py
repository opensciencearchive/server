"""RunIngester — runs ingester container on IngestStarted or continuation."""

import json
import logging
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
from osa.infrastructure.storage.layout import StorageLayout

logger = logging.getLogger(__name__)


class RunIngester(EventHandler[IngestStarted]):
    """Runs ingester container and emits IngesterBatchReady per batch."""

    __claim_timeout__ = 3600.0  # Ingester runs can be long

    ingest_repo: IngestRunRepository
    convention_service: ConventionService
    ingester_runner: IngesterRunner
    outbox: Outbox
    layout: StorageLayout

    async def handle(self, event: IngestStarted) -> None:
        ingest_run = await self.ingest_repo.get(event.ingest_run_srn)
        if ingest_run is None:
            raise NotFoundError(f"Ingest run not found: {event.ingest_run_srn}")

        # Transition to RUNNING on first ingester pull
        if ingest_run.status == IngestStatus.PENDING:
            ingest_run.mark_running()
            await self.ingest_repo.save(ingest_run)

        convention = await self.convention_service.get_convention(
            ConventionSRN.parse(event.convention_srn)
        )
        if convention.ingester is None:
            raise NotFoundError(f"No ingester for convention {event.convention_srn}")

        # Determine batch index from current batches_sourced
        batch_index = ingest_run.batches_sourced

        # Prepare scratch directory
        batch_dir = self.layout.ingest_batch_ingester_dir(event.ingest_run_srn, batch_index)
        batch_dir.mkdir(parents=True, exist_ok=True)

        # Load session state for continuation
        session_file = self.layout.ingest_session_file(event.ingest_run_srn)
        session = None
        if session_file.exists():
            session = json.loads(session_file.read_text())

        # Run ingester container
        inputs = IngesterInputs(
            convention_srn=convention.srn,
            config=convention.ingester.config,
            limit=ingest_run.batch_size,
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

        # Write records.jsonl to batch ingester dir
        records_file = batch_dir / "records.jsonl"
        with records_file.open("w") as f:
            for record in output.records:
                f.write(json.dumps(record) + "\n")

        # Save session for continuation
        if output.session:
            session_file.parent.mkdir(parents=True, exist_ok=True)
            session_file.write_text(json.dumps(output.session))

        has_more = output.session is not None and len(output.records) > 0

        # Update counters atomically
        await self.ingest_repo.increment_batches_sourced(
            event.ingest_run_srn,
            set_source_finished=not has_more,
        )

        # Emit batch ready event
        await self.outbox.append(
            IngesterBatchReady(
                id=EventId(uuid4()),
                ingest_run_srn=event.ingest_run_srn,
                batch_index=batch_index,
                has_more=has_more,
            )
        )

        logger.info(
            "Ingester batch %d ready for %s (%d records, has_more=%s)",
            batch_index,
            event.ingest_run_srn,
            len(output.records),
            has_more,
        )

        # Emit continuation event for next batch
        if has_more:
            await self.outbox.append(
                IngestStarted(
                    id=EventId(uuid4()),
                    ingest_run_srn=event.ingest_run_srn,
                    convention_srn=event.convention_srn,
                    batch_size=ingest_run.batch_size,
                )
            )
