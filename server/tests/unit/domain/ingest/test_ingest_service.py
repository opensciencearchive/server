"""T022: Unit tests for IngestService.start_ingest."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from osa.domain.ingest.model.ingest_run import IngestStatus
from osa.domain.ingest.service.ingest import IngestService
from osa.domain.shared.error import ConflictError, NotFoundError
from osa.domain.shared.model.source import IngesterDefinition
from osa.domain.shared.model.srn import Domain


def _make_convention(*, has_ingester: bool = True):
    conv = MagicMock()
    conv.srn = "urn:osa:localhost:conv:test-conv@1.0.0"
    conv.ingester = (
        IngesterDefinition(
            image="ghcr.io/example/ingester:v1",
            digest="sha256:abc123",
        )
        if has_ingester
        else None
    )
    return conv


def _make_service(
    *,
    convention=None,
    running_ingest=None,
    convention_not_found: bool = False,
) -> IngestService:
    ingest_repo = AsyncMock()
    ingest_repo.get_running_for_convention.return_value = running_ingest
    ingest_repo.save = AsyncMock()

    convention_service = AsyncMock()
    if convention_not_found:
        convention_service.get_convention.side_effect = NotFoundError("Convention not found")
    else:
        convention_service.get_convention.return_value = convention or _make_convention()

    outbox = AsyncMock()

    return IngestService(
        ingest_repo=ingest_repo,
        convention_service=convention_service,
        outbox=outbox,
        node_domain=Domain("localhost"),
    )


class TestStartIngest:
    @pytest.mark.asyncio
    async def test_creates_pending_ingest(self) -> None:
        service = _make_service()
        run = await service.start_ingest(
            convention_srn="urn:osa:localhost:conv:test-conv@1.0.0",
        )
        assert run.status == IngestStatus.PENDING
        assert run.convention_srn == "urn:osa:localhost:conv:test-conv@1.0.0"
        assert run.batch_size == 1000

    @pytest.mark.asyncio
    async def test_saves_and_emits_events(self) -> None:
        service = _make_service()
        run = await service.start_ingest(
            convention_srn="urn:osa:localhost:conv:test-conv@1.0.0",
        )
        service.ingest_repo.save.assert_called_once()
        assert service.outbox.append.call_count == 2

        # First event: IngestRunStarted (observability)
        first_event = service.outbox.append.call_args_list[0][0][0]
        assert first_event.__class__.__name__ == "IngestRunStarted"
        assert first_event.ingest_run_id == run.id
        assert first_event.convention_srn == run.convention_srn

        # Second event: NextBatchRequested (triggers first batch)
        second_event = service.outbox.append.call_args_list[1][0][0]
        assert second_event.__class__.__name__ == "NextBatchRequested"
        assert second_event.ingest_run_id == run.id
        assert second_event.convention_srn == run.convention_srn

    @pytest.mark.asyncio
    async def test_custom_batch_size(self) -> None:
        service = _make_service()
        run = await service.start_ingest(
            convention_srn="urn:osa:localhost:conv:test-conv@1.0.0",
            batch_size=500,
        )
        assert run.batch_size == 500

    @pytest.mark.asyncio
    async def test_rejects_convention_not_found(self) -> None:
        service = _make_service(convention_not_found=True)
        with pytest.raises(NotFoundError):
            await service.start_ingest(
                convention_srn="urn:osa:localhost:conv:nonexistent@1.0.0",
            )

    @pytest.mark.asyncio
    async def test_rejects_no_ingester_configured(self) -> None:
        service = _make_service(convention=_make_convention(has_ingester=False))
        with pytest.raises(NotFoundError, match="No ingester configured"):
            await service.start_ingest(
                convention_srn="urn:osa:localhost:conv:test-conv@1.0.0",
            )

    @pytest.mark.asyncio
    async def test_rejects_ingest_already_running(self) -> None:
        existing = MagicMock()
        service = _make_service(running_ingest=existing)
        with pytest.raises(ConflictError, match="already running"):
            await service.start_ingest(
                convention_srn="urn:osa:localhost:conv:test-conv@1.0.0",
            )
