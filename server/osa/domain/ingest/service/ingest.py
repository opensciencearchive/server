"""IngestService — orchestrates ingest lifecycle."""

import logging
from datetime import UTC, datetime
from uuid import uuid4

from osa.domain.deposition.service.convention import ConventionService
from osa.domain.ingest.event.events import IngestStarted
from osa.domain.ingest.model.ingest_run import IngestRun, IngestStatus
from osa.domain.ingest.port.repository import IngestRunRepository
from osa.domain.shared.error import ConflictError, NotFoundError
from osa.domain.shared.event import EventId
from osa.domain.shared.model.srn import ConventionSRN, Domain
from osa.domain.shared.outbox import Outbox
from osa.domain.shared.service import Service

logger = logging.getLogger(__name__)


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

        srn = f"urn:osa:{self.node_domain.root}:ing:{uuid4()}"
        now = datetime.now(UTC)

        ingest_run = IngestRun(
            srn=srn,
            convention_srn=convention_srn,
            status=IngestStatus.PENDING,
            batch_size=batch_size,
            started_at=now,
        )

        await self.ingest_repo.save(ingest_run)

        await self.outbox.append(
            IngestStarted(
                id=EventId(uuid4()),
                ingest_run_srn=srn,
                convention_srn=convention_srn,
                batch_size=batch_size,
            )
        )

        logger.info("Ingest started: %s for convention %s", srn, convention_srn)
        return ingest_run
