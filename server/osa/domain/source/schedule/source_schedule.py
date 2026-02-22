"""SourceSchedule - scheduled task that emits SourceRequested events."""

import logging
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from osa.domain.shared.event import EventId, Schedule
from osa.domain.shared.model.srn import ConventionSRN
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
        """Emit a SourceRequested event for the given convention.

        Params:
            convention: Convention SRN string
            limit: Optional limit on records to fetch
        """
        convention_srn = ConventionSRN.parse(params["convention"])
        limit: int | None = params.get("limit")

        # Look up last completed run for this convention
        last_run = await self.outbox.find_latest(SourceRunCompleted)

        since = None
        if last_run is not None and last_run.convention_srn == convention_srn:
            since = last_run.completed_at

        logger.info(
            "Scheduled source run: convention=%s (since=%s, limit=%s)",
            convention_srn,
            since,
            limit,
        )

        await self.outbox.append(
            SourceRequested(
                id=EventId(uuid4()),
                convention_srn=convention_srn,
                since=since,
                limit=limit,
            )
        )
