"""Unit tests for ValidationService — hook execution orchestration."""

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from osa.domain.shared.model.hook import (
    ColumnDef,
    HookName,
    OciConfig,
    TableFeatureSpec,
)
from osa.domain.shared.model.srn import DepositionSRN, Domain
from osa.domain.validation.model import RunStatus
from osa.domain.shared.failure import FailureKind, FailurePolicy, RuntimeFailure
from osa.domain.validation.model.hook import Hook
from osa.domain.validation.model.hook_release import HookRelease, HookReleaseId
from osa.domain.validation.model.hook_result import HookResult, HookStatus
from osa.domain.validation.model.hook_input import HookRecord
from osa.domain.validation.model.hook_run import HookRunStatus
from osa.domain.validation.port.hook_runner import HookInputs
from osa.domain.validation.service.validation import ValidationService


def _make_hook(name: str = "pocket_detect") -> Hook:
    # #145: a hook's identity is name + feature; runtime lives on its release.
    return Hook(
        name=HookName(name),
        feature=TableFeatureSpec(
            cardinality="many",
            columns=[ColumnDef(name="score", json_type="number", required=True)],
        ),
        live_release_id=HookReleaseId(uuid4()),
        created_at=datetime.now(UTC),
    )


def _make_release(name: str = "pocket_detect") -> HookRelease:
    return HookRelease(
        id=HookReleaseId(uuid4()),
        hook_name=HookName(name),
        version=1,
        runtime=OciConfig(
            image="ghcr.io/example/hook",
            digest="sha256:abc123",
        ),
        source_ref="git:abc123",
        built_at=datetime.now(UTC),
    )


def _make_registry(names: list[str]) -> AsyncMock:
    """Fake HookRegistryService resolving each name to a Hook + live HookRelease."""
    hooks = {HookName(n): _make_hook(n) for n in names}
    releases = {HookName(n): _make_release(n) for n in names}

    registry = AsyncMock()
    registry.resolve_live.return_value = releases

    async def get_hook(name: HookName) -> Hook | None:
        return hooks.get(name)

    registry.get_hook.side_effect = get_hook
    return registry


def _make_hook_result(
    name: str = "pocket_detect",
    status: HookStatus = HookStatus.PASSED,
) -> HookResult:
    return HookResult(
        hook_name=name,
        status=status,
        duration_seconds=1.5,
    )


def _make_service(
    run_repo: AsyncMock | None = None,
    hook_runner: AsyncMock | None = None,
    hook_storage: MagicMock | None = None,
    hook_registry: AsyncMock | None = None,
) -> ValidationService:
    hs = hook_storage or MagicMock()
    if not hasattr(hs, "get_hook_output_dir") or not callable(hs.get_hook_output_dir):
        hs.get_hook_output_dir = MagicMock(return_value=Path("/tmp/hooks/test"))
    if not hasattr(hs, "get_files_dir") or not callable(hs.get_files_dir):
        hs.get_files_dir = MagicMock(return_value=Path("/tmp/files/test"))
    # write_checkpoint / write_batch_outcomes / write_run_ref / write_hook_log are async
    hs.write_checkpoint = AsyncMock()
    hs.write_batch_outcomes = AsyncMock()
    hs.write_run_ref = AsyncMock()
    hs.write_hook_log = AsyncMock(return_value="/tmp/hooks/test/output/hook.log")
    return ValidationService(
        run_repo=run_repo or AsyncMock(),
        hook_runner=hook_runner or AsyncMock(),
        hook_storage=hs,
        hook_registry=hook_registry or _make_registry(["pocket_detect"]),
        failure_policy=FailurePolicy(),
        node_domain=Domain("localhost"),
    )


def _make_inputs() -> HookInputs:
    return HookInputs(
        records=[HookRecord(id="urn:osa:localhost:dep:test123", metadata={"name": "test"})],
        run_id="localhost_test123",
        files_dirs={"urn:osa:localhost:dep:test123": Path("/tmp/staging/files")},
    )


def _make_dep_srn() -> DepositionSRN:
    return DepositionSRN.parse("urn:osa:localhost:dep:test123")


class TestValidationServiceCreateRun:
    @pytest.mark.asyncio
    async def test_creates_pending_run(self):
        run_repo = AsyncMock()
        service = _make_service(run_repo=run_repo)

        run = await service.create_run(inputs=_make_inputs())
        assert run.status == RunStatus.PENDING
        assert run.results == []
        run_repo.save.assert_called_once()


class TestValidationServiceRunHooks:
    @pytest.mark.asyncio
    async def test_all_hooks_pass(self):
        hook_runner = AsyncMock()
        hook_runner.run.return_value = _make_hook_result()
        run_repo = AsyncMock()

        service = _make_service(run_repo, hook_runner)
        run = await service.create_run(inputs=_make_inputs())

        run, results = await service.run_hooks(
            run=run,
            deposition_srn=_make_dep_srn(),
            inputs=_make_inputs(),
            hook_names=[HookName("pocket_detect")],
        )

        assert run.status == RunStatus.COMPLETED
        assert len(results) == 1
        assert results[0].status == HookStatus.PASSED

    @pytest.mark.asyncio
    async def test_hook_rejected_halts_pipeline(self):
        hook_runner = AsyncMock()
        hook_runner.run.return_value = _make_hook_result(
            status=HookStatus.REJECTED,
        )
        registry = _make_registry(["hook1", "hook2"])
        service = _make_service(hook_runner=hook_runner, hook_registry=registry)
        run = await service.create_run(inputs=_make_inputs())

        run, results = await service.run_hooks(
            run=run,
            deposition_srn=_make_dep_srn(),
            inputs=_make_inputs(),
            hook_names=[HookName("hook1"), HookName("hook2")],
        )

        assert run.status == RunStatus.REJECTED
        assert len(results) == 1  # Second hook never ran

    @pytest.mark.asyncio
    async def test_hook_failed_halts_pipeline(self):
        hook_runner = AsyncMock()
        hook_runner.run.side_effect = RuntimeFailure(
            FailureKind.HOOK_EXIT,
            "Hook exited with code 1",
            exit_code=1,
            container_logs="traceback: boom",
        )
        registry = _make_registry(["pocket_detect"])
        service = _make_service(hook_runner=hook_runner, hook_registry=registry)
        run = await service.create_run(inputs=_make_inputs())

        run, results = await service.run_hooks(
            run=run,
            deposition_srn=_make_dep_srn(),
            inputs=_make_inputs(),
            hook_names=[HookName("pocket_detect")],
        )

        assert run.status == RunStatus.FAILED
        # The failed container's logs are persisted to a tenant-scoped artifact and
        # the locator stamped on the ERROR provenance run (#145/#147).
        service.hook_storage.write_hook_log.assert_awaited_once()
        assert service.hook_storage.write_hook_log.await_args.args[1] == "traceback: boom"
        recorded_run = registry.record_run.await_args.args[0]
        assert recorded_run.status == HookRunStatus.ERROR
        assert recorded_run.log_ref == "/tmp/hooks/test/output/hook.log"

    @pytest.mark.asyncio
    async def test_output_dir_from_hook_storage(self):
        """ValidationService gets output_dir from hook_storage.get_hook_output_dir."""
        hook_runner = AsyncMock()
        hook_runner.run.return_value = _make_hook_result()
        hook_storage = MagicMock()
        hook_storage.get_hook_output_dir.return_value = Path("/cold/hooks/pocket_detect")
        hook_storage.get_files_dir = MagicMock(return_value=Path("/tmp/files/test"))

        service = _make_service(hook_runner=hook_runner, hook_storage=hook_storage)
        run = await service.create_run(inputs=_make_inputs())

        dep_srn = _make_dep_srn()
        await service.run_hooks(
            run=run,
            deposition_srn=dep_srn,
            inputs=_make_inputs(),
            hook_names=[HookName("pocket_detect")],
        )

        hook_storage.get_hook_output_dir.assert_called_once_with(dep_srn, "pocket_detect")
        # Runner receives (hook, release, inputs, output_dir) — output_dir is the cold path.
        call_args = hook_runner.run.call_args
        assert call_args[0][3] == Path("/cold/hooks/pocket_detect")

    @pytest.mark.asyncio
    async def test_sequential_execution_order(self):
        """Hooks run in order; first pass before second starts."""
        call_order = []

        async def run_hook(hook, release, inputs, output_dir):
            call_order.append(hook.name.root)
            return _make_hook_result(name=hook.name.root)

        hook_runner = AsyncMock()
        hook_runner.run.side_effect = run_hook

        registry = _make_registry(["hook_a", "hook_b"])
        service = _make_service(hook_runner=hook_runner, hook_registry=registry)
        run = await service.create_run(inputs=_make_inputs())

        run, results = await service.run_hooks(
            run=run,
            deposition_srn=_make_dep_srn(),
            inputs=_make_inputs(),
            hook_names=[HookName("hook_a"), HookName("hook_b")],
        )

        assert call_order == ["hook_a", "hook_b"]
        assert len(results) == 2

    @pytest.mark.asyncio
    async def test_validation_service_halts_on_oom(self):
        """REGRESSION: OOM with exhausted retries should halt pipeline as FAILED."""
        hook_runner = AsyncMock()
        hook_runner.run.side_effect = RuntimeFailure(FailureKind.OOM, "Hook killed by OOM")
        registry = _make_registry(["pocket_detect"])
        service = _make_service(hook_runner=hook_runner, hook_registry=registry)
        run = await service.create_run(inputs=_make_inputs())

        run, results = await service.run_hooks(
            run=run,
            deposition_srn=_make_dep_srn(),
            inputs=_make_inputs(),
            hook_names=[HookName("pocket_detect")],
        )

        assert run.status == RunStatus.FAILED
        # The real OOM-retry count must reach provenance — not a hardcoded 0.
        # run_hook re-raises the OOM failure with oom_retries after exhausting
        # the policy's memory-bump budget (default 3).
        recorded_run = registry.record_run.await_args.args[0]
        assert recorded_run.status == HookRunStatus.ERROR
        assert recorded_run.oom_retries == 3

    @pytest.mark.asyncio
    async def test_validation_service_retries_on_oom(self):
        """OOM should be retried via HookService; PASSED on retry → COMPLETED."""
        call_count = 0

        async def run_hook(hook, release, inputs, output_dir):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeFailure(FailureKind.OOM, "Hook killed by OOM")
            return HookResult(
                hook_name=hook.name.root,
                status=HookStatus.PASSED,
                duration_seconds=5.0,
            )

        hook_runner = AsyncMock()
        hook_runner.run.side_effect = run_hook
        registry = _make_registry(["pocket_detect"])
        service = _make_service(hook_runner=hook_runner, hook_registry=registry)
        run = await service.create_run(inputs=_make_inputs())

        run, results = await service.run_hooks(
            run=run,
            deposition_srn=_make_dep_srn(),
            inputs=_make_inputs(),
            hook_names=[HookName("pocket_detect")],
        )

        assert run.status == RunStatus.COMPLETED
        assert call_count == 2
        # The one OOM retry is threaded into the provenance record (#145, fix #3).
        registry.record_run.assert_awaited_once()
        recorded_run = registry.record_run.await_args.args[0]
        assert recorded_run.oom_retries == 1
