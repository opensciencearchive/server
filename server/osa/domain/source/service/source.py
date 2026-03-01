"""SourceService - orchestrates running OCI source containers."""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from osa.domain.shared.event import EventId
from osa.domain.shared.model.source import SourceDefinition
from osa.domain.shared.model.srn import ConventionSRN
from osa.domain.shared.outbox import Outbox
from osa.domain.shared.service import Service
from osa.domain.source.event.source_record_ready import SourceRecordReady
from osa.domain.source.event.source_requested import SourceRequested
from osa.domain.source.event.source_run_completed import SourceRunCompleted
from osa.domain.source.port.source_runner import SourceInputs, SourceRunner
from osa.domain.source.port.storage import SourceStoragePort

logger = logging.getLogger(__name__)


@dataclass
class SourceResult:
    """Result of a source run."""

    convention_srn: ConventionSRN
    record_count: int
    started_at: datetime
    completed_at: datetime


class SourceService(Service):
    """Orchestrates running source containers.

    For each record produced by the source container, emits a
    SourceRecordReady event. The deposition domain handles creating
    depositions from these events.
    """

    source_runner: SourceRunner
    source_storage: SourceStoragePort
    outbox: Outbox

    async def run_source(
        self,
        convention_srn: ConventionSRN,
        source: SourceDefinition,
        since: datetime | None = None,
        limit: int | None = None,
        offset: int = 0,
        chunk_size: int = 1000,
        session: dict[str, Any] | None = None,
    ) -> SourceResult:
        """Run a source container and emit events for each produced record."""
        started_at = datetime.now(UTC)
        run_id = str(uuid4())[:12]

        logger.info(
            "Starting source run for %s (run=%s, since=%s, limit=%s, offset=%s)",
            convention_srn,
            run_id,
            since,
            limit,
            offset,
        )

        # Prepare dirs
        staging_dir = self.source_storage.get_source_staging_dir(convention_srn, run_id)
        work_dir = self.source_storage.get_source_output_dir(convention_srn, run_id)

        # Build inputs
        inputs = SourceInputs(
            config=source.config,
            since=since,
            limit=limit,
            offset=offset,
            session=session,
        )

        # Run container
        output = await self.source_runner.run(
            source=source,
            inputs=inputs,
            files_dir=staging_dir,
            work_dir=work_dir,
        )

        # Emit per-record events
        count = 0
        for record_data in output.records:
            source_id = record_data.get("source_id", "")
            metadata = record_data.get("metadata", {})
            file_paths = record_data.get("file_paths", [])

            await self.outbox.append(
                SourceRecordReady(
                    id=EventId(uuid4()),
                    convention_srn=convention_srn,
                    metadata=metadata,
                    file_paths=file_paths,
                    source_id=source_id,
                    staging_dir=str(staging_dir),
                )
            )
            count += 1

            if count % 100 == 0:
                logger.info("  Emitted %d SourceRecordReady events so far...", count)

        completed_at = datetime.now(UTC)
        is_final_chunk = output.session is None or count == 0

        logger.info(
            "Source run completed: %d records (run=%s, is_final=%s)",
            count,
            run_id,
            is_final_chunk,
        )

        # Emit continuation if session exists
        if not is_final_chunk:
            next_offset = offset + count
            logger.info("Emitting continuation event, next_offset=%d", next_offset)
            await self.outbox.append(
                SourceRequested(
                    id=EventId(uuid4()),
                    convention_srn=convention_srn,
                    since=since,
                    limit=limit,
                    offset=next_offset,
                    chunk_size=chunk_size,
                    session=output.session,
                )
            )

        await self.outbox.append(
            SourceRunCompleted(
                id=EventId(uuid4()),
                convention_srn=convention_srn,
                started_at=started_at,
                completed_at=completed_at,
                record_count=count,
                is_final_chunk=is_final_chunk,
            )
        )

        return SourceResult(
            convention_srn=convention_srn,
            record_count=count,
            started_at=started_at,
            completed_at=completed_at,
        )
