"""Tests for DepositionService.return_to_draft().

The return-to-draft transition used to be reachable via a ``ReturnToDraft`` event
handler; that handler was deleted when the pipeline collapsed into orchestrated
workflows (#160). The service method it delegated to lives on — ProcessSubmission
calls it directly on the validation-failed / on-exhausted paths.
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from osa.domain.auth.model.value import UserId
from osa.domain.deposition.model.aggregate import Deposition
from osa.domain.deposition.model.value import DepositionStatus
from osa.domain.shared.model.srn import ConventionSlug, DepositionSRN


def _make_dep_srn() -> DepositionSRN:
    return DepositionSRN.parse("urn:osa:localhost:dep:test-dep")


def _make_conv_slug() -> ConventionSlug:
    return ConventionSlug("test-conv")


def _make_deposition(status: DepositionStatus = DepositionStatus.IN_VALIDATION) -> Deposition:
    return Deposition(
        srn=_make_dep_srn(),
        convention_id=_make_conv_slug(),
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
