"""Scoped read access to the node's sign-in configuration (#184).

`GET /auth/config` (provider + ORCID client_id + admin ORCIDs; never the secret)
is gated by `auth:read` so a hosted control plane's read-proxy can surface it to
the dashboard with a per-node scoped token — while ADMIN / self-host access is
preserved.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from osa.domain.auth.model.identity import Anonymous
from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.auth.model.value import ProviderIdentity, UserId
from osa.domain.auth.query.get_auth_config import (
    GetAuthConfig,
    GetAuthConfigHandler,
)
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


def _config(client_id: str = "APP-123", admins: list[str] | None = None) -> MagicMock:
    """A config exposing just what the handler reads."""
    return SimpleNamespace(
        auth=SimpleNamespace(
            providers=SimpleNamespace(orcid=SimpleNamespace(client_id=client_id)),
            admins=SimpleNamespace(orcid=admins or ["0000-0002-1825-0097"]),
        )
    )


def test_gate_requires_auth_read() -> None:
    assert GetAuthConfigHandler.__auth__ == requires_scope("auth:read")


class TestGetAuthConfigAuth:
    @pytest.mark.asyncio
    async def test_allows_matching_scope(self) -> None:
        handler = GetAuthConfigHandler(
            principal=_principal(scopes=frozenset({"auth:read"})),
            config=_config(),  # type: ignore[arg-type]
        )
        result = await handler.run(GetAuthConfig())
        assert result.provider == "orcid"
        assert result.client_id == "APP-123"
        assert result.admin_orcids == ["0000-0002-1825-0097"]

    @pytest.mark.asyncio
    async def test_allows_admin_without_scope(self) -> None:
        handler = GetAuthConfigHandler(
            principal=_principal(roles=frozenset({Role.ADMIN})),
            config=_config(),  # type: ignore[arg-type]
        )
        assert (await handler.run(GetAuthConfig())).client_id == "APP-123"

    @pytest.mark.asyncio
    async def test_denies_wrong_scope(self) -> None:
        handler = GetAuthConfigHandler(
            principal=_principal(scopes=frozenset({"stats:read"})),
            config=_config(),  # type: ignore[arg-type]
        )
        with pytest.raises(AuthorizationError) as exc:
            await handler.run(GetAuthConfig())
        assert exc.value.code == "access_denied"

    @pytest.mark.asyncio
    async def test_denies_anonymous(self) -> None:
        handler = GetAuthConfigHandler(
            principal=Anonymous(),  # type: ignore[arg-type]
            config=_config(),  # type: ignore[arg-type]
        )
        with pytest.raises(AuthorizationError) as exc:
            await handler.run(GetAuthConfig())
        assert exc.value.code == "missing_token"
