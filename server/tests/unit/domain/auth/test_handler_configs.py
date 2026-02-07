"""Tests for concrete handler auth configurations.

Verifies that production handlers enforce their declared __auth__ gates
end-to-end (real handler classes, mocked services).
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from osa.domain.auth.command.assign_role import (
    AssignRole,
    AssignRoleHandler,
)
from osa.domain.auth.command.login import (
    InitiateLogin,
    InitiateLoginHandler,
)
from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.auth.model.value import ProviderIdentity, UserId
from osa.domain.auth.service.token import TokenService
from osa.domain.deposition.command.create import (
    CreateDeposition,
    CreateDepositionHandler,
)
from osa.domain.shared.error import AuthorizationError


def _make_principal(
    roles: frozenset[Role],
    user_id: UserId | None = None,
) -> Principal:
    return Principal(
        user_id=user_id or UserId.generate(),
        provider_identity=ProviderIdentity(provider="test", external_id="ext"),
        roles=roles,
    )


class TestCreateDepositionHandlerAuth:
    @pytest.mark.asyncio
    async def test_create_deposition_allows_depositor(self) -> None:
        depositor = _make_principal(frozenset({Role.DEPOSITOR}))
        service = AsyncMock()
        handler = CreateDepositionHandler(
            principal=depositor,
            deposition_service=service,
        )

        result = await handler.run(CreateDeposition())
        assert result.srn is not None

    @pytest.mark.asyncio
    async def test_create_deposition_rejects_unauthenticated(self) -> None:
        handler = CreateDepositionHandler.__new__(CreateDepositionHandler)

        with pytest.raises(AuthorizationError) as exc_info:
            await handler.run(CreateDeposition())
        assert exc_info.value.code == "missing_token"


class TestAssignRoleHandlerAuth:
    @pytest.mark.asyncio
    async def test_assign_role_allows_superadmin(self) -> None:
        superadmin = _make_principal(frozenset({Role.SUPERADMIN}))
        service = AsyncMock()
        # Mock the return value to match what the handler expects
        from datetime import UTC, datetime

        from osa.domain.auth.model.role_assignment import RoleAssignment, RoleAssignmentId

        target_user_id = UserId.generate()
        service.assign_role.return_value = RoleAssignment(
            id=RoleAssignmentId.generate(),
            user_id=target_user_id,
            role=Role.CURATOR,
            assigned_by=superadmin.user_id,
            assigned_at=datetime.now(UTC),
        )

        handler = AssignRoleHandler(
            principal=superadmin,
            authorization_service=service,
        )

        result = await handler.run(AssignRole(user_id=str(target_user_id), role="curator"))
        assert result.role == "curator"

    @pytest.mark.asyncio
    async def test_assign_role_rejects_admin(self) -> None:
        admin = _make_principal(frozenset({Role.ADMIN}))
        service = AsyncMock()
        handler = AssignRoleHandler(
            principal=admin,
            authorization_service=service,
        )

        with pytest.raises(AuthorizationError) as exc_info:
            await handler.run(AssignRole(user_id=str(UserId.generate()), role="curator"))
        assert exc_info.value.code == "access_denied"


class TestInitiateLoginHandlerAuth:
    @pytest.mark.asyncio
    async def test_public_login_handler_works_without_principal(self) -> None:
        provider_registry = MagicMock()
        identity_provider = MagicMock()
        identity_provider.get_authorization_url.return_value = "https://example.com/auth"
        provider_registry.get.return_value = identity_provider

        from osa.config import JwtConfig

        token_service = TokenService(
            _config=JwtConfig(
                secret="test-secret-key-256-bits-long-xx",
                algorithm="HS256",
                access_token_expire_minutes=60,
                refresh_token_expire_days=7,
            )
        )

        handler = InitiateLoginHandler(
            provider_registry=provider_registry,
            token_service=token_service,
        )

        result = await handler.run(
            InitiateLogin(
                callback_url="http://localhost/callback",
                final_redirect_uri="http://localhost/dashboard",
                provider="orcid",
            )
        )
        assert result.authorization_url == "https://example.com/auth"
