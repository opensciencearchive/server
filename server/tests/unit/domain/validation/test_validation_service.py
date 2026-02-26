"""Unit tests for ValidationService — hook execution orchestration."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from osa.domain.shared.model.hook import ColumnDef
from osa.domain.shared.model.hook_snapshot import HookSnapshot
from osa.domain.shared.model.srn import DepositionSRN, Domain
from osa.domain.validation.model import RunStatus
from osa.domain.validation.model.hook_result import HookResult, HookStatus
from osa.domain.validation.port.hook_runner import HookInputs
from osa.domain.validation.service.validation import ValidationService


def _make_hook_snapshot(name: str = "pocket_detect") -> HookSnapshot:
    return HookSnapshot(
        name=name,
        image="ghcr.io/example/hook",
        digest="sha256:abc123",
        features=[ColumnDef(name="score", json_type="number", required=True)],
        config={},
    )


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
) -> ValidationService:
    hs = hook_storage or MagicMock()
    if not hasattr(hs, "get_hook_output_dir") or not callable(hs.get_hook_output_dir):
        hs.get_hook_output_dir = MagicMock(return_value=Path("/tmp/hooks/test"))
    return ValidationService(
        run_repo=run_repo or AsyncMock(),
        hook_runner=hook_runner or AsyncMock(),
        hook_storage=hs,
        node_domain=Domain("localhost"),
    )


def _make_inputs() -> HookInputs:
    return HookInputs(
        record_json={"srn": "urn:osa:localhost:dep:test123", "metadata": {"name": "test"}},
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

        hook = _make_hook_snapshot()
        run, results = await service.run_hooks(
            run=run,
            deposition_srn=_make_dep_srn(),
            inputs=_make_inputs(),
            hooks=[hook],
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
        service = _make_service(hook_runner=hook_runner)
        run = await service.create_run(inputs=_make_inputs())

        hooks = [_make_hook_snapshot("hook1"), _make_hook_snapshot("hook2")]
        run, results = await service.run_hooks(
            run=run,
            deposition_srn=_make_dep_srn(),
            inputs=_make_inputs(),
            hooks=hooks,
        )

        assert run.status == RunStatus.REJECTED
        assert len(results) == 1  # Second hook never ran

    @pytest.mark.asyncio
    async def test_hook_failed_halts_pipeline(self):
        hook_runner = AsyncMock()
        hook_runner.run.return_value = _make_hook_result(
            status=HookStatus.FAILED,
        )
        service = _make_service(hook_runner=hook_runner)
        run = await service.create_run(inputs=_make_inputs())

        run, results = await service.run_hooks(
            run=run,
            deposition_srn=_make_dep_srn(),
            inputs=_make_inputs(),
            hooks=[_make_hook_snapshot()],
        )

        assert run.status == RunStatus.FAILED

    @pytest.mark.asyncio
    async def test_output_dir_from_hook_storage(self):
        """ValidationService gets output_dir from hook_storage.get_hook_output_dir."""
        hook_runner = AsyncMock()
        hook_runner.run.return_value = _make_hook_result()
        hook_storage = MagicMock()
        hook_storage.get_hook_output_dir.return_value = Path("/cold/hooks/pocket_detect")

        service = _make_service(hook_runner=hook_runner, hook_storage=hook_storage)
        run = await service.create_run(inputs=_make_inputs())

        dep_srn = _make_dep_srn()
        await service.run_hooks(
            run=run,
            deposition_srn=dep_srn,
            inputs=_make_inputs(),
            hooks=[_make_hook_snapshot()],
        )

        hook_storage.get_hook_output_dir.assert_called_once_with(dep_srn, "pocket_detect")
        # Runner receives the cold storage output_dir
        call_args = hook_runner.run.call_args
        assert call_args[0][2] == Path("/cold/hooks/pocket_detect")

    @pytest.mark.asyncio
    async def test_sequential_execution_order(self):
        """Hooks run in order; first pass before second starts."""
        call_order = []

        async def run_hook(hook, inputs, output_dir):
            call_order.append(hook.manifest.name)
            return _make_hook_result(name=hook.manifest.name)

        hook_runner = AsyncMock()
        hook_runner.run.side_effect = run_hook

        service = _make_service(hook_runner=hook_runner)
        run = await service.create_run(inputs=_make_inputs())

        hooks = [_make_hook_snapshot("hook_a"), _make_hook_snapshot("hook_b")]
        run, results = await service.run_hooks(
            run=run,
            deposition_srn=_make_dep_srn(),
            inputs=_make_inputs(),
            hooks=hooks,
        )

        assert call_order == ["hook_a", "hook_b"]
        assert len(results) == 2
