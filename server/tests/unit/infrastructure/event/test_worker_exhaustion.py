"""Tests for worker on_exhausted error safety.

Verifies that mark_failed is always called even when on_exhausted raises,
by testing the Worker._poll_once flow end-to-end with a handler that
raises PermanentError from handle() and RuntimeError from on_exhausted().
"""

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from osa.domain.ingest.event.events import NextBatchRequested
from osa.domain.ingest.handler.run_ingester import RunIngester
from osa.domain.ingest.model.ingest_run import IngestRunId
from osa.domain.shared.event import EventId
from osa.infrastructure.event.worker import Worker


class TestWorkerOnExhaustedErrorSafety:
    @pytest.mark.asyncio
    async def test_mark_failed_runs_even_if_on_exhausted_raises(self) -> None:
        """If on_exhausted throws, mark_failed must still run."""
        worker = Worker(RunIngester)

        # Mock handler whose on_exhausted raises
        handler = AsyncMock(spec=RunIngester)
        handler.handle = AsyncMock(side_effect=Exception("something broke"))
        handler.on_exhausted = AsyncMock(side_effect=RuntimeError("DB down during on_exhausted"))

        outbox = AsyncMock()

        # Create a delivery that has exhausted retries
        event = NextBatchRequested(
            id=EventId(uuid4()),
            ingest_run_id=IngestRunId("run-1"),
            convention_srn="urn:osa:localhost:conv:test@1.0.0",
            batch_size=100,
        )
        delivery = MagicMock()
        delivery.id = "delivery-1"
        delivery.event = event
        delivery.retry_count = 100  # exceeds __max_retries__

        claim_result = MagicMock()
        claim_result.deliveries = [delivery]
        claim_result.events = [event]
        claim_result.claimed_at = datetime.now(UTC)
        outbox.claim.return_value = claim_result

        # Wire up the DI scope mock
        async def mock_get(t: type) -> Any:
            if t is RunIngester:
                return handler
            return outbox

        scope = AsyncMock()
        scope.get = mock_get

        container = MagicMock()
        container.return_value.__aenter__ = AsyncMock(return_value=scope)
        container.return_value.__aexit__ = AsyncMock(return_value=False)

        worker.set_container(container)
        await worker._poll_once()

        # on_exhausted was called (and raised)
        handler.on_exhausted.assert_called_once()
        # mark_failed was STILL called despite the exception
        outbox.mark_failed.assert_called_once_with("delivery-1", "something broke")
