"""RecordService - orchestrates record creation from approved depositions."""

import logging
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from osa.domain.record.event.record_published import RecordPublished
from osa.domain.record.model.aggregate import Record
from osa.domain.record.port.repository import RecordRepository
from osa.domain.shared.event import EventId
from osa.domain.shared.model.hook_snapshot import HookSnapshot
from osa.domain.shared.model.srn import (
    ConventionSRN,
    DepositionSRN,
    Domain,
    LocalId,
    RecordSRN,
    RecordVersion,
)
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.outbox import Outbox
from osa.domain.shared.service import Service

logger = logging.getLogger(__name__)


class RecordService(Service):
    """Creates and persists Record aggregates from approved depositions."""

    record_repo: RecordRepository
    outbox: Outbox
    node_domain: Domain

    async def get(self, srn: RecordSRN) -> Record:
        """Retrieve a published record by SRN."""
        record = await self.record_repo.get(srn)
        if record is None:
            raise NotFoundError(f"Record not found: {srn}")
        return record

    async def publish_record(
        self,
        deposition_srn: DepositionSRN,
        metadata: dict[str, Any],
        convention_srn: ConventionSRN | None = None,
        hooks: list[HookSnapshot] | None = None,
        files_dir: str = "",
    ) -> Record:
        """Create and persist a Record from an approved deposition."""
        logger.debug(f"Creating record for approved deposition: {deposition_srn}")

        record_srn = RecordSRN(
            domain=self.node_domain,
            id=LocalId(str(uuid4())),
            version=RecordVersion(1),
        )

        record = Record(
            srn=record_srn,
            deposition_srn=deposition_srn,
            metadata=metadata,
            published_at=datetime.now(UTC),
        )

        await self.record_repo.save(record)
        logger.debug(f"Record persisted: {record_srn}")

        published = RecordPublished(
            id=EventId(uuid4()),
            record_srn=record_srn,
            deposition_srn=deposition_srn,
            metadata=metadata,
            convention_srn=convention_srn,
            hooks=hooks or [],
            files_dir=files_dir,
        )
        await self.outbox.append(published)

        logger.debug(f"RecordPublished event emitted: {record_srn}")
        return record
