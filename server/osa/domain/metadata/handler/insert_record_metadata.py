"""InsertRecordMetadata — writes a record's typed metadata row on RecordPublished."""

from __future__ import annotations

import logging

from osa.domain.metadata.service.metadata import MetadataService
from osa.domain.record.event.record_published import RecordPublished
from osa.domain.shared.event import EventHandler

logger = logging.getLogger(__name__)


class InsertRecordMetadata(EventHandler[RecordPublished]):
    """Reacts to RecordPublished, inserts a typed metadata row for the record."""

    metadata_service: MetadataService

    async def handle(self, event: RecordPublished) -> None:
        await self.metadata_service.insert(
            schema_srn=event.schema_srn,
            record_srn=event.record_srn,
            values=event.metadata,
        )
        logger.debug(
            "Inserted metadata row: record=%s schema=%s",
            event.record_srn,
            event.schema_srn,
        )
