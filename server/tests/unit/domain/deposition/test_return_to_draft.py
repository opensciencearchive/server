"""Unit tests for ReturnToDraft event handler."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from osa.domain.auth.model.value import UserId
from osa.domain.deposition.handler.return_to_draft import ReturnToDraft
from osa.domain.deposition.model.aggregate import Deposition
from osa.domain.deposition.model.value import DepositionStatus
from osa.domain.shared.event import EventId
from osa.domain.shared.model.srn import ConventionSRN, DepositionSRN
from osa.domain.validation.event.validation_failed import ValidationFailed


def _make_dep_srn(id: str = "test-dep") -> DepositionSRN:
    return DepositionSRN.parse(f"urn:osa:localhost:dep:{id}")


def _make_conv_srn() -> ConventionSRN:
    return ConventionSRN.parse("urn:osa:localhost:conv:test@1.0.0")


class TestReturnToDraft:
    @pytest.mark.asyncio
    async def test_returns_deposition_to_draft(self):
        dep = Deposition(
            srn=_make_dep_srn(),
            convention_srn=_make_conv_srn(),
            status=DepositionStatus.IN_VALIDATION,
            owner_id=UserId(uuid4()),
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
        )
        dep_repo = AsyncMock()
        dep_repo.get.return_value = dep

        handler = ReturnToDraft(deposition_repo=dep_repo)
        event = ValidationFailed(
            id=EventId(uuid4()),
            deposition_srn=dep.srn,
            reasons=["Missing required field"],
        )
        await handler.handle(event)

        assert dep.status == DepositionStatus.DRAFT
        dep_repo.save.assert_called_once_with(dep)

    @pytest.mark.asyncio
    async def test_handles_missing_deposition(self):
        dep_repo = AsyncMock()
        dep_repo.get.return_value = None

        handler = ReturnToDraft(deposition_repo=dep_repo)
        event = ValidationFailed(
            id=EventId(uuid4()),
            deposition_srn=_make_dep_srn(),
            reasons=["error"],
        )
        # Should not raise — workers must be resilient
        await handler.handle(event)
        dep_repo.save.assert_not_called()
