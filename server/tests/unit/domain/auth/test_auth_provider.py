"""Tests for identity resolution and AuthProvider (get_principal)."""

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import jwt as pyjwt
import pytest

from osa.config import JwtConfig
from osa.domain.auth.model.identity import Anonymous
from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.auth.model.value import ProviderIdentity, UserId
from osa.domain.auth.service.token import TokenService
from osa.domain.auth.util.di.provider import AuthProvider
from osa.domain.shared.error import AuthorizationError
from osa.util.di.fastapi import resolve_identity


def _make_jwt_config() -> JwtConfig:
    return JwtConfig(
        secret="test-secret-key-256-bits-long-xx",
        algorithm="HS256",
        access_token_expire_minutes=60,
        refresh_token_expire_days=7,
    )


def _make_token_service(config: JwtConfig | None = None) -> TokenService:
    return TokenService(_config=config or _make_jwt_config())


def _make_request(auth_header: str | None = None) -> MagicMock:
    request = MagicMock()
    headers: dict[str, str] = {}
    if auth_header is not None:
        headers["Authorization"] = auth_header
    request.headers = headers
    return request


def _make_valid_token(token_service: TokenService, user_id: UserId) -> str:
    return token_service.create_access_token(
        user_id=user_id,
        identity=ProviderIdentity(provider="orcid", external_id="0000-0001-2345-6789"),
    )


def _make_session_factory(role_names: list[str] | None = None) -> MagicMock:
    """Create a mock async_sessionmaker that returns role rows."""
    rows = [(name,) for name in (role_names or [])]
    mock_result = MagicMock()
    mock_result.__iter__ = lambda self: iter(rows)

    session = AsyncMock()
    session.execute.return_value = mock_result

    @asynccontextmanager
    async def session_ctx():
        yield session

    factory = MagicMock()
    factory.return_value = session_ctx()
    # Make factory callable multiple times
    factory.side_effect = lambda: session_ctx()
    return factory


class TestResolveIdentity:
    @pytest.mark.asyncio
    async def test_valid_jwt_returns_principal_with_roles(self) -> None:
        token_service = _make_token_service()
        user_id = UserId.generate()
        token = _make_valid_token(token_service, user_id)
        request = _make_request(f"Bearer {token}")
        session_factory = _make_session_factory(["curator"])

        identity = await resolve_identity(request, token_service, session_factory)

        assert isinstance(identity, Principal)
        assert identity.user_id == user_id
        assert identity.roles == frozenset({Role.CURATOR})

    @pytest.mark.asyncio
    async def test_expired_jwt_returns_anonymous(self) -> None:
        jwt_config = _make_jwt_config()
        token_service = _make_token_service(jwt_config)
        user_id = UserId.generate()

        # Create an expired token manually
        payload = {
            "sub": str(user_id),
            "provider": "orcid",
            "external_id": "0000-0001-2345-6789",
            "exp": datetime(2020, 1, 1, tzinfo=UTC),
        }
        token = pyjwt.encode(payload, jwt_config.secret, algorithm=jwt_config.algorithm)
        request = _make_request(f"Bearer {token}")
        session_factory = _make_session_factory()

        identity = await resolve_identity(request, token_service, session_factory)

        assert isinstance(identity, Anonymous)

    @pytest.mark.asyncio
    async def test_invalid_jwt_returns_anonymous(self) -> None:
        token_service = _make_token_service()
        request = _make_request("Bearer not-a-valid-jwt")
        session_factory = _make_session_factory()

        identity = await resolve_identity(request, token_service, session_factory)

        assert isinstance(identity, Anonymous)

    @pytest.mark.asyncio
    async def test_no_auth_header_returns_anonymous(self) -> None:
        token_service = _make_token_service()
        request = _make_request()
        session_factory = _make_session_factory()

        identity = await resolve_identity(request, token_service, session_factory)

        assert isinstance(identity, Anonymous)


class TestGetPrincipal:
    def test_get_principal_with_principal_returns_it(self) -> None:
        principal = Principal(
            user_id=UserId.generate(),
            provider_identity=ProviderIdentity(provider="orcid", external_id="ext"),
            roles=frozenset({Role.DEPOSITOR}),
        )

        provider = AuthProvider()
        result = provider.get_principal(principal)

        assert result is principal

    def test_get_principal_with_anonymous_raises_missing_token(self) -> None:
        provider = AuthProvider()

        with pytest.raises(AuthorizationError) as exc_info:
            provider.get_principal(Anonymous())
        assert exc_info.value.code == "missing_token"
