"""SourceSchedule - scheduled task that emits SourceRequested events."""

import logging
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from osa.domain.shared.event import EventId, Schedule
from osa.domain.shared.outbox import Outbox
from osa.domain.source.event.source_requested import SourceRequested
from osa.domain.source.event.source_run_completed import SourceRunCompleted

logger = logging.getLogger(__name__)


@dataclass
class SourceSchedule(Schedule):
    """Scheduled task that emits SourceRequested events.

    Looks up the last completed source run to determine the `since` timestamp,
    then emits a SourceRequested event to trigger a new pull.
    """

    outbox: Outbox

    async def run(self, **params: Any) -> None:
        """Emit a SourceRequested event for the given source.

        Params:
            source_name: Key into config.sources list (e.g., "geo-entrez")
            limit: Optional limit on records to fetch
        """
        source_name: str = params["source_name"]
        limit: int | None = params.get("limit")

        # Look up last completed run for this source
        last_run = await self.outbox.find_latest(SourceRunCompleted)

        # Only use last_run if it's for the same source
        since = None
        if last_run is not None and last_run.source_name == source_name:
            since = last_run.completed_at

        logger.info(f"Scheduled source run: {source_name} (since={since}, limit={limit})")

        await self.outbox.append(
            SourceRequested(
                id=EventId(uuid4()),
                source_name=source_name,
                since=since,
                limit=limit,
            )
        )
