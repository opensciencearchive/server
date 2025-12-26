"""RecordCreationListener - creates records when depositions are approved."""

import logging
from datetime import UTC, datetime
from uuid import uuid4

from osa.config import Config
from osa.domain.curation.event.deposition_approved import DepositionApproved
from osa.domain.record.event.record_published import RecordPublished
from osa.domain.record.model.aggregate import Record
from osa.domain.record.port.repository import RecordRepository
from osa.domain.shared.event import EventId, EventListener
from osa.domain.shared.model.srn import Domain, LocalId, RecordSRN, RecordVersion
from osa.domain.shared.outbox import Outbox

logger = logging.getLogger(__name__)


class ConvertDepositionToRecord(EventListener[DepositionApproved]):
    """Creates and persists records when depositions are approved."""

    record_repo: RecordRepository
    outbox: Outbox
    config: Config

    async def handle(self, event: DepositionApproved) -> None:
        """Create a Record from an approved deposition and emit RecordPublished."""
        logger.debug(f"Creating record for approved deposition: {event.deposition_srn}")

        domain = Domain(self.config.server.domain)

        # Create record SRN (version 1 for new records)
        record_srn = RecordSRN(
            domain=domain,
            id=LocalId(str(uuid4())),
            version=RecordVersion(1),
        )

        # Create the Record aggregate
        record = Record(
            srn=record_srn,
            deposition_srn=event.deposition_srn,
            metadata=event.metadata,
            published_at=datetime.now(UTC),
        )

        # Persist the record
        await self.record_repo.save(record)
        logger.debug(f"Record persisted: {record_srn}")

        # Emit RecordPublished for downstream consumers (indexing, etc.)
        published = RecordPublished(
            id=EventId(uuid4()),
            record_srn=record_srn,
            deposition_srn=event.deposition_srn,
            metadata=event.metadata,
        )
        await self.outbox.append(published)
        # Session commit handled by BackgroundWorker

        logger.debug(f"RecordPublished event emitted: {record_srn}")
