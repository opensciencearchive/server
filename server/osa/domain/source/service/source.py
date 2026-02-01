"""SourceService - orchestrates pulling from data sources."""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from osa.domain.deposition.event.submitted import DepositionSubmittedEvent
from osa.domain.shared.event import EventId
from osa.domain.shared.model.srn import DepositionSRN, Domain, LocalId
from osa.domain.shared.outbox import Outbox
from osa.domain.shared.service import Service
from osa.domain.source.event.source_requested import SourceRequested
from osa.domain.source.event.source_run_completed import SourceRunCompleted
from osa.domain.source.model.registry import SourceRegistry

logger = logging.getLogger(__name__)


@dataclass
class SourceResult:
    """Result of a source run."""

    source_name: str
    record_count: int
    started_at: datetime
    completed_at: datetime


class SourceService(Service):
    """Orchestrates pulling records from sources and emitting deposition events.

    This service encapsulates the business logic for pulling from sources that was
    previously embedded in the PullFromSource listener. It can be called from multiple
    entry points (event listeners, CLI commands, scheduled jobs).

    Supports chunked processing to enable:
    1. Committing after each chunk (progress saved)
    2. Emitting continuation events so downstream processing starts immediately
    3. Efficient pagination via session state (e.g., NCBI WebEnv)
    """

    sources: SourceRegistry
    outbox: Outbox
    node_domain: Domain

    async def run_source(
        self,
        source_name: str,
        since: datetime | None = None,
        limit: int | None = None,
        offset: int = 0,
        chunk_size: int = 1000,
        session: dict[str, Any] | None = None,
    ) -> SourceResult:
        """Pull records from a source and emit DepositionSubmitted events.

        Processes records in chunks, emitting continuation events for subsequent chunks.
        This allows downstream processing (indexing, validation) to start while
        source ingestion continues.

        Args:
            source_name: Name of the source to use.
            since: Only fetch records updated after this time.
            limit: Maximum total number of records to fetch (across all chunks).
            offset: Starting position for this chunk.
            chunk_size: Number of records to process per chunk.
            session: Opaque pagination state from previous chunk.

        Returns:
            SourceResult with run statistics for this chunk.

        Raises:
            ValueError: If the source is not found.
        """
        source = self.sources.get(source_name)
        if not source:
            raise ValueError(f"Unknown source: {source_name}")

        started_at = datetime.now(UTC)
        logger.info(
            f"Starting pull from {source_name}, since={since}, limit={limit}, "
            f"offset={offset}, chunk_size={chunk_size}"
        )

        # Calculate effective limit for this chunk
        # If we have an overall limit, cap to remaining records
        if limit is not None:
            remaining = limit - offset
            if remaining <= 0:
                # Already hit the limit - nothing more to fetch
                logger.info(f"Limit reached: offset={offset} >= limit={limit}, marking as final")
                completed_at = datetime.now(UTC)
                await self.outbox.append(
                    SourceRunCompleted(
                        id=EventId(uuid4()),
                        source_name=source_name,
                        source_type=source.name,
                        started_at=started_at,
                        completed_at=completed_at,
                        record_count=0,
                        since=since,
                        limit=limit,
                        offset=offset,
                        chunk_size=chunk_size,
                        is_final_chunk=True,
                    )
                )
                return SourceResult(
                    source_name=source_name,
                    record_count=0,
                    started_at=started_at,
                    completed_at=completed_at,
                )
            effective_chunk_size = min(chunk_size, remaining)
        else:
            effective_chunk_size = chunk_size

        # Fetch chunk_size + 1 to detect if more records exist
        fetch_limit = effective_chunk_size + 1

        # Call source with session, get updated session back
        records_iter, next_session = await source.pull(
            since=since,
            limit=fetch_limit,
            offset=offset,
            session=session,
        )

        count = 0
        has_more = False

        async for record in records_iter:
            # Check if we've reached our chunk limit
            if count >= effective_chunk_size:
                has_more = True
                break

            # Create a deposition SRN for this record
            dep_srn = DepositionSRN(
                domain=self.node_domain,
                id=LocalId(str(uuid4())),
            )

            # Emit DepositionSubmitted with the record metadata
            submitted_event = DepositionSubmittedEvent(
                id=EventId(uuid4()),
                deposition_id=dep_srn,
                metadata=record.metadata,
            )
            await self.outbox.append(submitted_event)
            count += 1

            # Log progress every 100 records
            if count % 100 == 0:
                logger.info(f"  Pulled {count} records so far (chunk offset={offset})...")

            title = record.metadata.get("title", "")[:60]
            logger.debug(f"  [{record.source_id}] {title}...")

        completed_at = datetime.now(UTC)
        is_final_chunk = not has_more

        logger.info(
            f"Chunk completed: {count} records from {source_name} "
            f"(offset={offset}, is_final={is_final_chunk})"
        )

        # Emit completion event for this chunk
        await self.outbox.append(
            SourceRunCompleted(
                id=EventId(uuid4()),
                source_name=source_name,
                source_type=source.name,
                started_at=started_at,
                completed_at=completed_at,
                record_count=count,
                since=since,
                limit=limit,
                offset=offset,
                chunk_size=chunk_size,
                is_final_chunk=is_final_chunk,
            )
        )

        # Emit continuation event if more records exist
        if not is_final_chunk:
            next_offset = offset + count
            logger.info(f"Emitting continuation event for {source_name}, next_offset={next_offset}")
            await self.outbox.append(
                SourceRequested(
                    id=EventId(uuid4()),
                    source_name=source_name,
                    since=since,
                    limit=limit,
                    offset=next_offset,
                    chunk_size=chunk_size,
                    session=next_session,
                )
            )

        return SourceResult(
            source_name=source_name,
            record_count=count,
            started_at=started_at,
            completed_at=completed_at,
        )
