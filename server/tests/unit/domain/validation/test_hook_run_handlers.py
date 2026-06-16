"""Unit tests for the hook-run provenance + logs read handlers (#147).

Services/storage are mocked; these assert handler orchestration, the ADMIN auth
gate, `has_logs` derivation, the never-leak-the-locator rule, and the 404 paths
(unknown run, and a run with no captured logs).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.auth.model.value import ProviderIdentity, UserId
from osa.domain.shared.error import AuthorizationError, NotFoundError
from osa.domain.validation.model.hook_release import HookReleaseId
from osa.domain.validation.model.hook_run import HookRun, HookRunId, HookRunStatus
from osa.domain.validation.query.get_hook_run import (
    GetHookRun,
    GetHookRunHandler,
    HookRunDetail,
)
from osa.domain.validation.query.get_hook_run_logs import (
    GetHookRunLogs,
    GetHookRunLogsHandler,
)

_T0 = datetime(2026, 1, 1, tzinfo=UTC)
RUN_ID = HookRunId(uuid4())


def _principal(role: Role = Role.ADMIN) -> Principal:
    return Principal(
        user_id=UserId.generate(),
        provider_identity=ProviderIdentity(provider="test", external_id="ext"),
        roles=frozenset({role}),
    )


def _run(*, log_ref: str | None) -> HookRun:
    return HookRun(
        id=RUN_ID,
        release_id=HookReleaseId(uuid4()),
        status=HookRunStatus.ERROR if log_ref else HookRunStatus.PASSED,
        started_at=_T0,
        finished_at=_T0,
        duration_s=1.0,
        oom_retries=0,
        log_ref=log_ref,
    )


class TestGetHookRunHandler:
    @pytest.mark.asyncio
    async def test_returns_detail_with_has_logs_true(self) -> None:
        service = AsyncMock()
        service.get_run.return_value = _run(log_ref="/data/runs/x/output/hook.log")

        handler = GetHookRunHandler(principal=_principal(), service=service)
        result: HookRunDetail = await handler.run(GetHookRun(run_id=RUN_ID))

        assert result.id == RUN_ID
        assert result.has_logs is True
        assert result.status == HookRunStatus.ERROR
        # The internal locator must never appear on the wire DTO.
        assert "log_ref" not in result.model_dump()
        assert "/data/runs" not in result.model_dump_json()

    @pytest.mark.asyncio
    async def test_passed_run_has_logs_false(self) -> None:
        service = AsyncMock()
        service.get_run.return_value = _run(log_ref=None)

        handler = GetHookRunHandler(principal=_principal(), service=service)
        result = await handler.run(GetHookRun(run_id=RUN_ID))

        assert result.has_logs is False

    @pytest.mark.asyncio
    async def test_unknown_run_404(self) -> None:
        service = AsyncMock()
        service.get_run.return_value = None

        handler = GetHookRunHandler(principal=_principal(), service=service)
        with pytest.raises(NotFoundError):
            await handler.run(GetHookRun(run_id=RUN_ID))

    @pytest.mark.asyncio
    async def test_requires_admin(self) -> None:
        handler = GetHookRunHandler(principal=_principal(Role.CURATOR), service=AsyncMock())
        with pytest.raises(AuthorizationError):
            await handler.run(GetHookRun(run_id=RUN_ID))


async def _drain(stream: AsyncIterator[bytes]) -> bytes:
    return b"".join([chunk async for chunk in stream])


class TestGetHookRunLogsHandler:
    @pytest.mark.asyncio
    async def test_streams_logs_from_storage(self) -> None:
        async def _bytes() -> AsyncIterator[bytes]:
            yield b"traceback: boom\n"

        service = AsyncMock()
        service.get_run.return_value = _run(log_ref="/data/runs/x/output/hook.log")
        storage = AsyncMock()
        storage.read_hook_log.return_value = _bytes()

        handler = GetHookRunLogsHandler(
            principal=_principal(), service=service, hook_storage=storage
        )
        result = await handler.run(GetHookRunLogs(run_id=RUN_ID))

        assert await _drain(result.stream) == b"traceback: boom\n"
        storage.read_hook_log.assert_awaited_once_with("/data/runs/x/output/hook.log")

    @pytest.mark.asyncio
    async def test_unknown_run_404(self) -> None:
        service = AsyncMock()
        service.get_run.return_value = None
        storage = AsyncMock()

        handler = GetHookRunLogsHandler(
            principal=_principal(), service=service, hook_storage=storage
        )
        with pytest.raises(NotFoundError):
            await handler.run(GetHookRunLogs(run_id=RUN_ID))
        storage.read_hook_log.assert_not_called()

    @pytest.mark.asyncio
    async def test_run_without_logs_404(self) -> None:
        service = AsyncMock()
        service.get_run.return_value = _run(log_ref=None)
        storage = AsyncMock()

        handler = GetHookRunLogsHandler(
            principal=_principal(), service=service, hook_storage=storage
        )
        with pytest.raises(NotFoundError):
            await handler.run(GetHookRunLogs(run_id=RUN_ID))
        storage.read_hook_log.assert_not_called()

    @pytest.mark.asyncio
    async def test_requires_admin(self) -> None:
        handler = GetHookRunLogsHandler(
            principal=_principal(Role.CURATOR), service=AsyncMock(), hook_storage=AsyncMock()
        )
        with pytest.raises(AuthorizationError):
            await handler.run(GetHookRunLogs(run_id=RUN_ID))
