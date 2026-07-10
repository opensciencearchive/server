"""Tests for GetIngestion — a failed run carries a queryable explanation (#152)."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.auth.model.value import ProviderIdentity, UserId
from osa.domain.ingest.model.ingest_run import IngestRun, IngestRunId, IngestStatus
from osa.domain.ingest.query.get_ingestion import GetIngestion, GetIngestionHandler
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.failure import FailureKind

_T0 = datetime(2026, 1, 1, tzinfo=UTC)


def _principal(role: Role = Role.ADMIN) -> Principal:
    return Principal(
        user_id=UserId.generate(),
        provider_identity=ProviderIdentity(provider="test", external_id="ext"),
        roles=frozenset({role}),
    )


def _make_run(**overrides) -> IngestRun:
    defaults = {
        "id": IngestRunId("run-1"),
        "convention_id": "test-conv",
        "status": IngestStatus.RUNNING,
        "batch_size": 100,
        "started_at": _T0,
    }
    defaults.update(overrides)
    return IngestRun(**defaults)


class TestGetIngestionHandler:
    @pytest.mark.asyncio
    async def test_exposes_failure_reason_and_kind(self) -> None:
        service = AsyncMock()
        service.get_ingestion.return_value = _make_run(
            status=IngestStatus.FAILED,
            failure_reason="Image pull failed: 401 Unauthorized",
            failure_kind=FailureKind.IMAGE_PULL,
            completed_at=_T0,
        )
        handler = GetIngestionHandler(principal=_principal(), service=service)

        result = await handler.run(GetIngestion(ingest_run_id=IngestRunId("run-1")))

        assert result.status == IngestStatus.FAILED
        assert result.failure_reason == "Image pull failed: 401 Unauthorized"
        assert result.failure_kind is FailureKind.IMAGE_PULL

    @pytest.mark.asyncio
    async def test_healthy_run_has_no_failure_fields(self) -> None:
        service = AsyncMock()
        service.get_ingestion.return_value = _make_run(
            batches_ingested=2, batches_completed=1, published_count=1000
        )
        handler = GetIngestionHandler(principal=_principal(), service=service)

        result = await handler.run(GetIngestion(ingest_run_id=IngestRunId("run-1")))

        assert result.failure_reason is None
        assert result.failure_kind is None
        assert result.batches_ingested == 2
        assert result.published_count == 1000

    @pytest.mark.asyncio
    async def test_missing_run_raises_not_found(self) -> None:
        service = AsyncMock()
        service.get_ingestion.side_effect = NotFoundError("Ingest run not found: nope")
        handler = GetIngestionHandler(principal=_principal(), service=service)

        with pytest.raises(NotFoundError):
            await handler.run(GetIngestion(ingest_run_id=IngestRunId("nope")))


class TestGetIngestionService:
    @pytest.mark.asyncio
    async def test_service_raises_not_found_for_missing_run(self) -> None:
        from osa.domain.ingest.service.ingest import IngestService
        from osa.domain.shared.model.srn import Domain

        repo = AsyncMock()
        repo.get.return_value = None
        service = IngestService(
            ingest_repo=repo,
            convention_service=AsyncMock(),
            outbox=AsyncMock(),
            node_domain=Domain("localhost"),
            instrumentation=MagicMock(),
        )

        with pytest.raises(NotFoundError):
            await service.get_ingestion(IngestRunId("missing"))
