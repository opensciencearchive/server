"""CreateDepositionFromSource — creates a deposition from a source record."""

import logging
from pathlib import Path

from osa.domain.auth.model.value import SYSTEM_USER_ID
from osa.domain.deposition.port.storage import FileStoragePort
from osa.domain.deposition.service.deposition import DepositionService
from osa.domain.shared.event import EventHandler
from osa.domain.source.event.source_record_ready import SourceRecordReady

logger = logging.getLogger(__name__)


class CreateDepositionFromSource(EventHandler[SourceRecordReady]):
    """Creates a deposition when a source record is ready.

    Replaces the direct DepositionService calls that used to live
    in SourceService — now the source domain only emits events and
    the deposition domain reacts.
    """

    deposition_service: DepositionService
    file_storage: FileStoragePort

    async def handle(self, event: SourceRecordReady) -> None:
        """Create deposition, set metadata, move files, and submit."""
        dep = await self.deposition_service.create(
            convention_srn=event.convention_srn,
            owner_id=SYSTEM_USER_ID,
        )

        await self.deposition_service.update_metadata(
            srn=dep.srn,
            metadata=event.metadata,
        )

        self.file_storage.move_source_files_to_deposition(
            staging_dir=Path(event.staging_dir),
            source_id=event.source_id,
            deposition_srn=dep.srn,
        )

        await self.deposition_service.submit(srn=dep.srn)

        logger.info(
            "Created deposition %s from source record %s",
            dep.srn,
            event.source_id,
        )
