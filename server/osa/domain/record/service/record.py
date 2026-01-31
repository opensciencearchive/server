"""RecordService - orchestrates record creation from approved depositions."""

import logging
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from osa.domain.record.event.record_published import RecordPublished
from osa.domain.record.model.aggregate import Record
from osa.domain.record.port.repository import RecordRepository
from osa.domain.shared.event import EventId
from osa.domain.shared.model.srn import (
    DepositionSRN,
    Domain,
    LocalId,
    RecordSRN,
    RecordVersion,
)
from osa.domain.shared.outbox import Outbox
from osa.domain.shared.service import Service

logger = logging.getLogger(__name__)


class RecordService(Service):
    """Creates and persists Record aggregates from approved depositions.

    This service encapsulates the business logic for record creation that was
    previously embedded in the ConvertDepositionToRecord listener. It can be
    called from multiple entry points (event listeners, CLI commands, APIs).
    """

    record_repo: RecordRepository
    outbox: Outbox
    node_domain: Domain

    async def publish_record(
        self,
        deposition_srn: DepositionSRN,
        metadata: dict[str, Any],
    ) -> Record:
        """Create and persist a Record from an approved deposition.

        Args:
            deposition_srn: SRN of the approved deposition.
            metadata: The record metadata.

        Returns:
            The created Record aggregate.
        """
        logger.debug(f"Creating record for approved deposition: {deposition_srn}")

        # Create record SRN (version 1 for new records)
        record_srn = RecordSRN(
            domain=self.node_domain,
            id=LocalId(str(uuid4())),
            version=RecordVersion(1),
        )

        # Create the Record aggregate
        record = Record(
            srn=record_srn,
            deposition_srn=deposition_srn,
            metadata=metadata,
            published_at=datetime.now(UTC),
        )

        # Persist the record
        await self.record_repo.save(record)
        logger.debug(f"Record persisted: {record_srn}")

        # Emit RecordPublished for downstream consumers (indexing, etc.)
        published = RecordPublished(
            id=EventId(uuid4()),
            record_srn=record_srn,
            deposition_srn=deposition_srn,
            metadata=metadata,
        )
        await self.outbox.append(published)

        logger.debug(f"RecordPublished event emitted: {record_srn}")

        return record
