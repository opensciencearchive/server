"""Tests for AuthProvider identity resolution (get_identity / get_principal)."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import jwt as pyjwt
import pytest

from osa.config import JwtConfig
from osa.domain.auth.model.identity import Anonymous
from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.auth.model.role_assignment import RoleAssignment, RoleAssignmentId
from osa.domain.auth.model.value import ProviderIdentity, UserId
from osa.domain.auth.service.token import TokenService
from osa.domain.auth.util.di.provider import AuthProvider
from osa.domain.shared.error import AuthorizationError


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


def _make_role_repo(assignments: list[RoleAssignment] | None = None) -> AsyncMock:
    repo = AsyncMock()
    repo.get_by_user_id.return_value = assignments or []
    return repo


class TestGetIdentity:
    @pytest.mark.asyncio
    async def test_valid_jwt_returns_principal_with_roles(self) -> None:
        token_service = _make_token_service()
        user_id = UserId.generate()
        token = _make_valid_token(token_service, user_id)
        request = _make_request(f"Bearer {token}")

        assignment = RoleAssignment(
            id=RoleAssignmentId.generate(),
            user_id=user_id,
            role=Role.CURATOR,
            assigned_by=UserId.generate(),
            assigned_at=datetime.now(UTC),
        )
        role_repo = _make_role_repo([assignment])

        provider = AuthProvider()
        identity = await provider.get_identity(request, token_service, role_repo)

        assert isinstance(identity, Principal)
        assert identity.user_id == user_id
        assert identity.roles == frozenset({Role.CURATOR})

    @pytest.mark.asyncio
    async def test_expired_jwt_returns_anonymous(self) -> None:
        config = _make_jwt_config()
        token_service = _make_token_service(config)
        user_id = UserId.generate()

        # Create an expired token manually
        payload = {
            "sub": str(user_id),
            "provider": "orcid",
            "external_id": "0000-0001-2345-6789",
            "exp": datetime(2020, 1, 1, tzinfo=UTC),
        }
        token = pyjwt.encode(payload, config.secret, algorithm=config.algorithm)
        request = _make_request(f"Bearer {token}")
        role_repo = _make_role_repo()

        provider = AuthProvider()
        identity = await provider.get_identity(request, token_service, role_repo)

        assert isinstance(identity, Anonymous)

    @pytest.mark.asyncio
    async def test_invalid_jwt_returns_anonymous(self) -> None:
        token_service = _make_token_service()
        request = _make_request("Bearer not-a-valid-jwt")
        role_repo = _make_role_repo()

        provider = AuthProvider()
        identity = await provider.get_identity(request, token_service, role_repo)

        assert isinstance(identity, Anonymous)

    @pytest.mark.asyncio
    async def test_no_auth_header_returns_anonymous(self) -> None:
        token_service = _make_token_service()
        request = _make_request()
        role_repo = _make_role_repo()

        provider = AuthProvider()
        identity = await provider.get_identity(request, token_service, role_repo)

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
