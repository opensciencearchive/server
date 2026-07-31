"""Scoped M2M read access to the operational ingest surfaces (#184).

`GET /ingestions` (list + detail) and `GET /ingesters` are gated by a single
OAuth read scope each, so a properly-scoped M2M token reads exactly its surface
without conferring ADMIN elsewhere. RequiresScope authorizes on scope OR ADMIN,
so existing ADMIN/self-host access is preserved.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from osa.domain.auth.model.identity import Anonymous
from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.auth.model.value import ProviderIdentity, UserId
from osa.domain.deposition.query.list_ingesters import (
    ListIngesters,
    ListIngestersHandler,
)
from osa.domain.ingest.query.get_ingestion import GetIngestion, GetIngestionHandler
from osa.domain.ingest.query.list_ingestions import (
    ListIngestions,
    ListIngestionsHandler,
)
from osa.domain.ingest.model.ingest_run import IngestRunId
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


class TestGateDeclarations:
    def test_list_ingestions_requires_ingestions_read(self) -> None:
        assert ListIngestionsHandler.__auth__ == requires_scope("ingestions:read")

    def test_get_ingestion_requires_ingestions_read(self) -> None:
        assert GetIngestionHandler.__auth__ == requires_scope("ingestions:read")

    def test_list_ingesters_requires_ingesters_read(self) -> None:
        assert ListIngestersHandler.__auth__ == requires_scope("ingesters:read")


class TestListIngestionsAuth:
    @pytest.mark.asyncio
    async def test_allows_matching_scope(self) -> None:
        service = AsyncMock()
        service.list_ingestions.return_value = []
        handler = ListIngestionsHandler(
            principal=_principal(scopes=frozenset({"ingestions:read"})),
            service=service,
        )
        result = await handler.run(ListIngestions())
        assert result.items == []

    @pytest.mark.asyncio
    async def test_allows_admin_without_scope(self) -> None:
        service = AsyncMock()
        service.list_ingestions.return_value = []
        handler = ListIngestionsHandler(
            principal=_principal(roles=frozenset({Role.ADMIN})),
            service=service,
        )
        assert (await handler.run(ListIngestions())).items == []

    @pytest.mark.asyncio
    async def test_denies_wrong_scope(self) -> None:
        # A stats:read token cannot read the ingestions surface.
        handler = ListIngestionsHandler(
            principal=_principal(scopes=frozenset({"stats:read"})),
            service=AsyncMock(),
        )
        with pytest.raises(AuthorizationError) as exc:
            await handler.run(ListIngestions())
        assert exc.value.code == "access_denied"

    @pytest.mark.asyncio
    async def test_denies_anonymous(self) -> None:
        handler = ListIngestionsHandler(principal=Anonymous(), service=AsyncMock())  # type: ignore[arg-type]
        with pytest.raises(AuthorizationError) as exc:
            await handler.run(ListIngestions())
        assert exc.value.code == "missing_token"


class TestGetIngestionAuth:
    @pytest.mark.asyncio
    async def test_denies_wrong_scope(self) -> None:
        handler = GetIngestionHandler(
            principal=_principal(scopes=frozenset({"ingesters:read"})),
            service=AsyncMock(),
        )
        with pytest.raises(AuthorizationError) as exc:
            await handler.run(GetIngestion(ingest_run_id=IngestRunId("run-1")))
        assert exc.value.code == "access_denied"


class TestListIngestersAuth:
    @pytest.mark.asyncio
    async def test_allows_matching_scope(self) -> None:
        service = AsyncMock()
        service.list_conventions_with_source.return_value = []
        handler = ListIngestersHandler(
            principal=_principal(scopes=frozenset({"ingesters:read"})),
            convention_service=service,
        )
        result = await handler.run(ListIngesters())
        assert result.items == []

    @pytest.mark.asyncio
    async def test_denies_wrong_scope(self) -> None:
        # An ingestions:read token cannot read the ingesters catalog.
        handler = ListIngestersHandler(
            principal=_principal(scopes=frozenset({"ingestions:read"})),
            convention_service=AsyncMock(),
        )
        with pytest.raises(AuthorizationError) as exc:
            await handler.run(ListIngesters())
        assert exc.value.code == "access_denied"
