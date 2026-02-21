"""Unit tests for ValidationService — hook execution orchestration."""

from unittest.mock import AsyncMock

import pytest

from osa.domain.shared.model.hook import (
    ColumnDef,
    FeatureSchema,
    HookDefinition,
    HookManifest,
)
from osa.domain.shared.model.srn import Domain
from osa.domain.validation.model import RunStatus
from osa.domain.validation.model.hook_result import HookResult, HookStatus
from osa.domain.validation.port.hook_runner import HookInputs
from osa.domain.validation.service.validation import ValidationService


def _make_hook_def(name: str = "pocket_detect") -> HookDefinition:
    return HookDefinition(
        image="ghcr.io/example/hook",
        digest="sha256:abc123",
        manifest=HookManifest(
            name=name,
            record_schema="SampleSchema",
            cardinality="one",
            feature_schema=FeatureSchema(
                columns=[
                    ColumnDef(name="score", json_type="number", required=True),
                ]
            ),
        ),
    )


def _make_hook_result(
    name: str = "pocket_detect",
    status: HookStatus = HookStatus.PASSED,
    features: list | None = None,
) -> HookResult:
    return HookResult(
        hook_name=name,
        status=status,
        features=features or [],
        duration_seconds=1.5,
    )


def _make_service(
    run_repo: AsyncMock | None = None,
    hook_runner: AsyncMock | None = None,
    feature_service: AsyncMock | None = None,
) -> ValidationService:
    return ValidationService(
        run_repo=run_repo or AsyncMock(),
        hook_runner=hook_runner or AsyncMock(),
        feature_service=feature_service or AsyncMock(),
        node_domain=Domain("localhost"),
    )


def _make_inputs() -> HookInputs:
    return HookInputs(
        record_json={"srn": "urn:osa:localhost:rec:test123", "name": "test"},
    )


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
        feature_service = AsyncMock()
        run_repo = AsyncMock()

        service = _make_service(run_repo, hook_runner, feature_service)
        run = await service.create_run(inputs=_make_inputs())

        hook = _make_hook_def()
        run, results = await service.run_hooks(
            run=run,
            convention_id="test-conv",
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

        hooks = [_make_hook_def("hook1"), _make_hook_def("hook2")]
        run, results = await service.run_hooks(
            run=run,
            convention_id="test-conv",
            inputs=_make_inputs(),
            hooks=hooks,
        )

        assert run.status == RunStatus.FAILED
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
            convention_id="test-conv",
            inputs=_make_inputs(),
            hooks=[_make_hook_def()],
        )

        assert run.status == RunStatus.FAILED

    @pytest.mark.asyncio
    async def test_features_inserted_on_success(self):
        features = [{"score": 0.95}]
        hook_runner = AsyncMock()
        hook_runner.run.return_value = _make_hook_result(features=features)
        feature_service = AsyncMock()

        service = _make_service(hook_runner=hook_runner, feature_service=feature_service)
        run = await service.create_run(inputs=_make_inputs())

        run, _ = await service.run_hooks(
            run=run,
            convention_id="test-conv",
            inputs=HookInputs(
                record_json={"srn": "urn:osa:localhost:rec:test123"},
            ),
            hooks=[_make_hook_def()],
        )

        feature_service.insert_features.assert_called_once_with(
            convention_id="test-conv",
            hook_name="pocket_detect",
            record_srn="urn:osa:localhost:rec:test123",
            rows=features,
        )

    @pytest.mark.asyncio
    async def test_no_features_skips_insert(self):
        hook_runner = AsyncMock()
        hook_runner.run.return_value = _make_hook_result(features=[])
        feature_service = AsyncMock()

        service = _make_service(hook_runner=hook_runner, feature_service=feature_service)
        run = await service.create_run(inputs=_make_inputs())

        run, _ = await service.run_hooks(
            run=run,
            convention_id="test-conv",
            inputs=_make_inputs(),
            hooks=[_make_hook_def()],
        )

        feature_service.insert_features.assert_not_called()

    @pytest.mark.asyncio
    async def test_sequential_execution_order(self):
        """Hooks run in order; first pass before second starts."""
        call_order = []

        async def run_hook(hook, inputs, workspace_dir):
            call_order.append(hook.manifest.name)
            return _make_hook_result(name=hook.manifest.name)

        hook_runner = AsyncMock()
        hook_runner.run.side_effect = run_hook

        service = _make_service(hook_runner=hook_runner)
        run = await service.create_run(inputs=_make_inputs())

        hooks = [_make_hook_def("hook_a"), _make_hook_def("hook_b")]
        run, results = await service.run_hooks(
            run=run,
            convention_id="test-conv",
            inputs=_make_inputs(),
            hooks=hooks,
        )

        assert call_order == ["hook_a", "hook_b"]
        assert len(results) == 2
