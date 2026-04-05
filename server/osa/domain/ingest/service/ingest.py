"""IngestService — orchestrates ingest lifecycle."""

from datetime import UTC, datetime
from uuid import uuid4

from osa.domain.deposition.service.convention import ConventionService
from osa.domain.ingest.event.events import IngestCompleted, IngestRunStarted, NextBatchRequested
from osa.domain.ingest.model.ingest_run import IngestRun, IngestRunId, IngestStatus
from osa.domain.ingest.port.repository import IngestRunRepository
from osa.domain.shared.error import ConflictError, NotFoundError
from osa.domain.shared.event import EventId
from osa.domain.shared.model.srn import ConventionSRN, Domain
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
        convention_srn: str,
        batch_size: int = 1000,
        limit: int | None = None,
    ) -> IngestRun:
        """Create an ingest run for a convention.

        Validates:
        - Convention exists
        - Convention has an ingester configured
        - No ingest is already running for this convention
        """
        parsed_srn = ConventionSRN.parse(convention_srn)
        convention = await self.convention_service.get_convention(parsed_srn)

        if convention.ingester is None:
            raise NotFoundError(
                f"No ingester configured for convention {convention_srn}",
                code="no_ingester_configured",
            )

        existing = await self.ingest_repo.get_running_for_convention(convention_srn)
        if existing is not None:
            raise ConflictError(
                f"Ingest already running for convention {convention_srn}",
                code="ingest_already_running",
            )

        run_id = IngestRunId(str(uuid4()))
        now = datetime.now(UTC)

        ingest_run = IngestRun(
            id=run_id,
            convention_srn=convention_srn,
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
                convention_srn=convention_srn,
                batch_size=batch_size,
            )
        )

        await self.outbox.append(
            NextBatchRequested(
                id=EventId(uuid4()),
                ingest_run_id=run_id,
                convention_srn=convention_srn,
                batch_size=batch_size,
            )
        )

        srn = f"urn:osa:{self.node_domain.root}:ing:{run_id}"
        log.info(
            "ingest started for {convention_srn}",
            ingest_run_srn=srn,
            convention_srn=convention_srn,
            batch_size=batch_size,
            limit=limit,
        )
        return ingest_run

    async def fail_batch(self, ingest_run_id: IngestRunId) -> None:
        """Account for a batch that permanently failed hook processing.

        Increments batches_failed and completes the run if all batches
        are now accounted for (completed + failed >= ingested).
        """
        ingest_run = await self.ingest_repo.increment_failed(ingest_run_id)
        await self._check_completion(ingest_run)

    async def fail_ingestion(self, ingest_run_id: IngestRunId) -> None:
        """Account for a failed ingester pull.

        The batch was never sourced, so we mark ingestion as finished
        (no more batches coming) and increment batches_failed. The
        completion condition can then fire based on whatever batches
        were already sourced.
        """
        await self.ingest_repo.increment_batches_ingested(
            ingest_run_id,
            set_ingestion_finished=True,
        )
        ingest_run = await self.ingest_repo.increment_failed(ingest_run_id)
        await self._check_completion(ingest_run)

    async def _check_completion(self, ingest_run: IngestRun) -> None:
        """Transition to COMPLETED and emit IngestCompleted if all batches are accounted for."""
        if not ingest_run.is_complete:
            return
        now = datetime.now(UTC)
        ingest_run.check_completion(now)
        await self.ingest_repo.save(ingest_run)
        await self.outbox.append(
            IngestCompleted(
                id=EventId(uuid4()),
                ingest_run_id=ingest_run.id,
                total_published=ingest_run.published_count,
            )
        )
