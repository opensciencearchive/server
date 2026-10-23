"""Scoped M2M write access to the ingest trigger (server#190).

`POST /ingestions` (start an ingestion run) is gated by the `ingestions:write`
OAuth scope, so the hosted control plane can broker CLI/dashboard-triggered
runs with a per-node, single-scope M2M token — without a tenant ORCID/admin
session. RequiresScope authorizes on scope OR ADMIN, so existing ADMIN and
self-host access is preserved.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from osa.domain.auth.model.identity import Anonymous
from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.auth.model.value import ProviderIdentity, UserId
from osa.domain.ingest.command.cancel_ingestion import (
    CancelIngestion,
    CancelIngestionHandler,
)
from osa.domain.ingest.command.start_ingest import StartIngest, StartIngestHandler
from osa.domain.shared.authorization.gate import requires_scope
from osa.domain.shared.error import AuthorizationError


def _principal(
    *, roles: frozenset[Role] = frozenset(), scopes: frozenset[str] = frozenset()
) -> Principal:
    return Principal(
        user_id=UserId.generate(),
        provider_identity=ProviderIdentity(provider="m2m", external_id="client-1"),
        roles=roles,
        scopes=scopes,
    )


def _ingest_run() -> object:
    from datetime import datetime, timezone

    run = AsyncMock()
    run.id = "ing_1"
    run.convention_id = "cultivarium"
    run.status = "pending"
    run.started_at = datetime(2026, 8, 1, tzinfo=timezone.utc)
    return run


def _service() -> AsyncMock:
    from osa.domain.shared.model.srn import Domain

    service = AsyncMock()
    service.start_ingest.return_value = _ingest_run()
    service.node_domain = Domain(root="cultivarium.amacr.in")
    return service


class TestGateDeclaration:
    def test_start_ingest_requires_ingestions_write(self) -> None:
        assert StartIngestHandler.__auth__ == requires_scope("ingestions:write")


class TestStartIngestAuth:
    async def test_allows_matching_scope(self) -> None:
        handler = StartIngestHandler(
            principal=_principal(scopes=frozenset({"ingestions:write"})),
            service=_service(),
        )
        result = await handler.run(StartIngest(convention_id="cultivarium"))
        assert result.convention_id == "cultivarium"

    async def test_allows_admin_without_scope(self) -> None:
        handler = StartIngestHandler(
            principal=_principal(roles=frozenset({Role.ADMIN})),
            service=_service(),
        )
        result = await handler.run(StartIngest(convention_id="cultivarium"))
        assert result.status == "pending"

    async def test_denies_wrong_scope(self) -> None:
        # A read-scoped token cannot trigger a run.
        handler = StartIngestHandler(
            principal=_principal(scopes=frozenset({"ingestions:read"})),
            service=_service(),
        )
        with pytest.raises(AuthorizationError) as exc:
            await handler.run(StartIngest(convention_id="cultivarium"))
        assert exc.value.code == "access_denied"

    async def test_denies_anonymous(self) -> None:
        handler = StartIngestHandler(principal=Anonymous(), service=_service())
        with pytest.raises(AuthorizationError) as exc:
            await handler.run(StartIngest(convention_id="cultivarium"))
        assert exc.value.code == "missing_token"


def _cancelling_service() -> AsyncMock:
    from datetime import datetime, timezone

    from osa.domain.ingest.model.ingest_run import IngestStatus

    run = _ingest_run()
    run.status = IngestStatus.CANCELLED
    run.failure_reason = "cancelled by operator"
    run.completed_at = datetime(2026, 8, 1, tzinfo=timezone.utc)
    service = AsyncMock()
    service.cancel_ingestion.return_value = run
    return service


class TestCancelIngestionAuth:
    """Stopping a run is the same blast radius as starting one (#227)."""

    def test_cancel_requires_ingestions_write(self) -> None:
        assert CancelIngestionHandler.__auth__ == requires_scope("ingestions:write")

    async def test_allows_matching_scope(self) -> None:
        handler = CancelIngestionHandler(
            principal=_principal(scopes=frozenset({"ingestions:write"})),
            service=_cancelling_service(),
        )
        result = await handler.run(CancelIngestion(ingest_run_id="ing_1"))
        assert result.status == "cancelled"

    async def test_allows_admin_without_scope(self) -> None:
        handler = CancelIngestionHandler(
            principal=_principal(roles=frozenset({Role.ADMIN})),
            service=_cancelling_service(),
        )
        result = await handler.run(CancelIngestion(ingest_run_id="ing_1"))
        assert result.id == "ing_1"

    async def test_denies_read_scope(self) -> None:
        handler = CancelIngestionHandler(
            principal=_principal(scopes=frozenset({"ingestions:read"})),
            service=_cancelling_service(),
        )
        with pytest.raises(AuthorizationError) as exc:
            await handler.run(CancelIngestion(ingest_run_id="ing_1"))
        assert exc.value.code == "access_denied"

    async def test_denies_anonymous(self) -> None:
        handler = CancelIngestionHandler(principal=Anonymous(), service=_cancelling_service())
        with pytest.raises(AuthorizationError) as exc:
            await handler.run(CancelIngestion(ingest_run_id="ing_1"))
        assert exc.value.code == "missing_token"
