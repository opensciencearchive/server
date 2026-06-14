"""Tests for RunHooks — OOM exhaustion should still emit HookBatchCompleted."""

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from osa.domain.ingest.event.events import HookBatchCompleted, IngesterBatchReady
from osa.domain.ingest.handler.run_hooks import RunHooks
from osa.domain.ingest.model.ingest_run import IngestRun, IngestRunId, IngestStatus
from osa.domain.shared.error import OOMError, PermanentError
from osa.domain.shared.event import EventId
from osa.domain.shared.model.hook import (
    HookName,
    OciConfig,
    OciLimits,
    TableFeatureSpec,
)
from osa.domain.validation.model.hook import Hook
from osa.domain.validation.model.hook_release import HookRelease, HookReleaseId


def _make_release(name: str = "pockets", memory: str = "1g") -> HookRelease:
    return HookRelease(
        id=HookReleaseId(uuid4()),
        hook_name=HookName(name),
        version=1,
        runtime=OciConfig(
            image="ghcr.io/test/pockets:v1",
            digest="sha256:abc123",
            limits=OciLimits(memory=memory),
        ),
        source_ref="git:abc123",
        built_at=datetime.now(UTC),
    )


def _make_hook(name: str = "pockets") -> Hook:
    return Hook(
        name=HookName(name),
        feature=TableFeatureSpec(cardinality="one", columns=[]),
        live_release_id=HookReleaseId(uuid4()),
        created_at=datetime.now(UTC),
    )


def _make_event(
    ingest_run_id: str = "run-1",
    batch_index: int = 0,
) -> IngesterBatchReady:
    return IngesterBatchReady(
        id=EventId(uuid4()),
        ingest_run_id=IngestRunId(ingest_run_id),
        batch_index=batch_index,
        has_more=False,
    )


def _make_convention():
    # Conventions now reference hooks by name (#145), not embedded identities.
    conv = AsyncMock()
    conv.hooks = [HookName("pockets")]
    return conv


def _make_hook_registry() -> AsyncMock:
    """Fake HookRegistryService resolving the single 'pockets' hook + release."""
    registry = AsyncMock()
    release = _make_release()
    registry.resolve_live.return_value = {HookName("pockets"): release}
    registry.get_hook.return_value = _make_hook()
    return registry


def _make_handler(*, hook_service_side_effect=None) -> RunHooks:
    ingest_repo = AsyncMock()
    ingest_repo.get.return_value = IngestRun(
        id=IngestRunId("run-1"),
        # Conventions are identified by a bare slug now (#145).
        convention_id="test-conv",
        status=IngestStatus.RUNNING,
        batch_size=100,
        started_at=datetime.now(UTC),
    )

    convention_service = AsyncMock()
    convention_service.get_convention.return_value = _make_convention()

    ingest_storage = AsyncMock()
    ingest_storage.read_records.return_value = [
        {"source_id": "rec-1", "metadata": {}, "files": []},
    ]
    ingest_storage.batch_files_dir.return_value = Path("/tmp/files")
    ingest_storage.hook_work_dir.return_value = Path("/tmp/work")

    hook_service = AsyncMock()
    if hook_service_side_effect:
        hook_service.run_hooks_for_batch.side_effect = hook_service_side_effect

    return RunHooks(
        ingest_repo=ingest_repo,
        ingest_service=AsyncMock(),
        convention_service=convention_service,
        hook_service=hook_service,
        hook_registry=_make_hook_registry(),
        outbox=AsyncMock(),
        ingest_storage=ingest_storage,
    )


class TestRunHooksOOMExhaustion:
    @pytest.mark.asyncio
    async def test_oom_exhaustion_emits_hook_batch_completed(self) -> None:
        """OOM exhaustion should still emit HookBatchCompleted so passed records get published."""
        handler = _make_handler(hook_service_side_effect=OOMError("OOM after 3 retries"))
        event = _make_event()

        await handler.handle(event)

        # HookBatchCompleted should be emitted (not swallowed)
        emitted_events = [call[0][0] for call in handler.outbox.append.call_args_list]
        assert any(isinstance(e, HookBatchCompleted) for e in emitted_events), (
            "HookBatchCompleted should be emitted on OOM exhaustion "
            "so PublishBatch can publish records that passed"
        )

    @pytest.mark.asyncio
    async def test_oom_exhaustion_does_not_fail_batch(self) -> None:
        """OOM exhaustion should not call _fail_batch — the batch has partial results."""
        handler = _make_handler(hook_service_side_effect=OOMError("OOM after 3 retries"))
        event = _make_event()

        await handler.handle(event)

        handler.ingest_service.fail_batch.assert_not_called()

    @pytest.mark.asyncio
    async def test_permanent_error_still_fails_batch(self) -> None:
        """Non-OOM PermanentError should still fail the batch (no partial results)."""
        error = PermanentError("image pull failed")
        handler = _make_handler(hook_service_side_effect=error)
        event = _make_event()

        await handler.handle(event)

        handler.ingest_service.fail_batch.assert_called_once()
        # HookBatchCompleted should NOT be emitted
        emitted_events = [call[0][0] for call in handler.outbox.append.call_args_list]
        assert not any(isinstance(e, HookBatchCompleted) for e in emitted_events)
