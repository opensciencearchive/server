"""RecordService - orchestrates record creation from any source."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from osa.domain.record.event.record_published import RecordPublished
from osa.domain.record.model.aggregate import Record
from osa.domain.record.model.draft import RecordDraft
from osa.domain.record.port.repository import RecordRepository
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.event import EventId
from osa.domain.shared.model.srn import (
    Domain,
    LocalId,
    RecordSRN,
    RecordVersion,
)
from osa.domain.shared.outbox import Outbox
from osa.domain.shared.service import Service

if TYPE_CHECKING:
    from osa.domain.record.port.feature_reader import FeatureReader

logger = logging.getLogger(__name__)


class RecordService(Service):
    """Creates and persists Record aggregates from any source."""

    record_repo: RecordRepository
    outbox: Outbox
    node_domain: Domain
    feature_reader: FeatureReader

    async def get_features_for_record(
        self, record_srn: RecordSRN
    ) -> dict[str, list[dict[str, Any]]]:
        """Fetch feature data for a record."""
        return await self.feature_reader.get_features_for_record(record_srn)

    async def get(self, srn: RecordSRN) -> Record:
        """Retrieve a published record by SRN."""
        record = await self.record_repo.get(srn)
        if record is None:
            raise NotFoundError(f"Record not found: {srn}")
        return record

    async def bulk_publish(self, drafts: list[RecordDraft]) -> list[Record]:
        """Bulk-publish records from an ingest batch.

        Uses save_many() for multi-row INSERT with ON CONFLICT DO NOTHING.
        Does NOT emit per-record RecordPublished events — the caller emits
        a single IngestBatchPublished event instead (AD-3).
        """
        if not drafts:
            return []

        records: list[Record] = []
        for draft in drafts:
            record_srn = RecordSRN(
                domain=self.node_domain,
                id=LocalId(str(uuid4())),
                version=RecordVersion(1),
            )
            records.append(
                Record(
                    srn=record_srn,
                    source=draft.source,
                    convention_srn=draft.convention_srn,
                    metadata=draft.metadata,
                    published_at=datetime.now(UTC),
                )
            )

        published = await self.record_repo.save_many(records)
        logger.info("Bulk-published %d records (of %d drafts)", len(published), len(drafts))
        return published

    async def publish_record(self, draft: RecordDraft) -> Record:
        """Create and persist a Record from a draft."""
        logger.info(f"Creating record from {draft.source.type} source: {draft.source.id}")

        record_srn = RecordSRN(
            domain=self.node_domain,
            id=LocalId(str(uuid4())),
            version=RecordVersion(1),
        )

        record = Record(
            srn=record_srn,
            source=draft.source,
            convention_srn=draft.convention_srn,
            metadata=draft.metadata,
            published_at=datetime.now(UTC),
        )

        await self.record_repo.save(record)
        logger.info(f"Record persisted: {record_srn}")

        published = RecordPublished(
            id=EventId(uuid4()),
            record_srn=record_srn,
            source=draft.source,
            convention_srn=draft.convention_srn,
            metadata=draft.metadata,
            expected_features=draft.expected_features,
        )
        await self.outbox.append(published)

        logger.info(f"RecordPublished event emitted: {record_srn}")
        return record
