"""Unit tests for ReturnToDraft event handler."""

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from osa.domain.deposition.handler.return_to_draft import ReturnToDraft
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.event import EventId
from osa.domain.shared.model.srn import ConventionSRN, DepositionSRN
from osa.domain.validation.event.validation_failed import ValidationFailed
from osa.domain.validation.model import RunStatus


def _make_dep_srn(id: str = "test-dep") -> DepositionSRN:
    return DepositionSRN.parse(f"urn:osa:localhost:dep:{id}")


def _make_conv_srn() -> ConventionSRN:
    return ConventionSRN.parse("urn:osa:localhost:conv:test@1.0.0")


class TestReturnToDraft:
    @pytest.mark.asyncio
    async def test_delegates_to_service(self):
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
    async def test_handles_missing_deposition(self):
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
        # Should not raise — workers must be resilient
        await handler.handle(event)
        service.return_to_draft.assert_called_once()
