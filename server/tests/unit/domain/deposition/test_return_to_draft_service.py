"""TDD Red: Tests for ReturnToDraft handler delegating to DepositionService."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from osa.domain.auth.model.value import UserId
from osa.domain.deposition.model.aggregate import Deposition
from osa.domain.deposition.model.value import DepositionStatus
from osa.domain.shared.event import EventId
from osa.domain.shared.model.srn import ConventionSRN, DepositionSRN
from osa.domain.validation.event.validation_failed import ValidationFailed
from osa.domain.validation.model import RunStatus


def _make_dep_srn() -> DepositionSRN:
    return DepositionSRN.parse("urn:osa:localhost:dep:test-dep")


def _make_conv_srn() -> ConventionSRN:
    return ConventionSRN.parse("urn:osa:localhost:conv:test@1.0.0")


def _make_deposition(status: DepositionStatus = DepositionStatus.IN_VALIDATION) -> Deposition:
    return Deposition(
        srn=_make_dep_srn(),
        convention_srn=_make_conv_srn(),
        status=status,
        owner_id=UserId(uuid4()),
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )


class TestDepositionServiceReturnToDraft:
    """DepositionService.return_to_draft() transitions deposition back to DRAFT."""

    @pytest.mark.asyncio
    async def test_returns_deposition_to_draft(self):
        from osa.domain.deposition.service.deposition import DepositionService

        dep = _make_deposition()
        repo = AsyncMock()
        repo.get.return_value = dep

        service = DepositionService(
            deposition_repo=repo,
            convention_repo=AsyncMock(),
            file_storage=AsyncMock(),
            outbox=AsyncMock(),
            node_domain=_make_dep_srn().domain,
        )

        await service.return_to_draft(dep.srn)

        assert dep.status == DepositionStatus.DRAFT
        repo.save.assert_called_once_with(dep)

    @pytest.mark.asyncio
    async def test_raises_not_found_for_missing_deposition(self):
        from osa.domain.deposition.service.deposition import DepositionService
        from osa.domain.shared.error import NotFoundError

        repo = AsyncMock()
        repo.get.return_value = None

        service = DepositionService(
            deposition_repo=repo,
            convention_repo=AsyncMock(),
            file_storage=AsyncMock(),
            outbox=AsyncMock(),
            node_domain=_make_dep_srn().domain,
        )

        with pytest.raises(NotFoundError):
            await service.return_to_draft(_make_dep_srn())


class TestReturnToDraftHandlerDelegatesToService:
    """ReturnToDraft handler delegates to deposition_service.return_to_draft()."""

    @pytest.mark.asyncio
    async def test_handler_delegates_to_service(self):
        from osa.domain.deposition.handler.return_to_draft import ReturnToDraft

        service = AsyncMock()
        handler = ReturnToDraft(deposition_service=service)

        event = ValidationFailed(
            id=EventId(uuid4()),
            deposition_srn=_make_dep_srn(),
            convention_srn=_make_conv_srn(),
            status=RunStatus.FAILED,
            reasons=["Missing required field"],
        )
        await handler.handle(event)

        service.return_to_draft.assert_called_once_with(_make_dep_srn())

    @pytest.mark.asyncio
    async def test_handler_catches_not_found(self):
        """Handler should not blow up if deposition is missing — workers must be resilient."""
        from osa.domain.deposition.handler.return_to_draft import ReturnToDraft
        from osa.domain.shared.error import NotFoundError

        service = AsyncMock()
        service.return_to_draft.side_effect = NotFoundError("not found")

        handler = ReturnToDraft(deposition_service=service)
        event = ValidationFailed(
            id=EventId(uuid4()),
            deposition_srn=_make_dep_srn(),
            convention_srn=_make_conv_srn(),
            status=RunStatus.FAILED,
            reasons=["error"],
        )
        # Should not raise
        await handler.handle(event)
