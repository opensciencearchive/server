"""Unit tests for admin bootstrapping via ORCiD list (US1)."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from osa.config import JwtConfig
from osa.domain.auth.model.role import Role
from osa.domain.auth.port.identity_provider import IdentityInfo
from osa.domain.auth.service.auth import AuthService
from osa.domain.auth.service.token import TokenService


def make_auth_service(
    user_repo: AsyncMock | None = None,
    linked_account_repo: AsyncMock | None = None,
    refresh_token_repo: AsyncMock | None = None,
    role_repo: AsyncMock | None = None,
    token_service: TokenService | None = None,
    outbox: AsyncMock | None = None,
    base_role: Role | None = None,
    device_auth_repo: AsyncMock | None = None,
    admin_orcids: set[str] | None = None,
) -> AuthService:
    """Create an AuthService with mocked dependencies."""
    if user_repo is None:
        user_repo = AsyncMock()
    if linked_account_repo is None:
        linked_account_repo = AsyncMock()
    if refresh_token_repo is None:
        refresh_token_repo = AsyncMock()
    if role_repo is None:
        role_repo = AsyncMock()
    if token_service is None:
        config = JwtConfig(
            secret="test-secret-key-256-bits-long-xx",
            algorithm="HS256",
            access_token_expire_minutes=60,
            refresh_token_expire_days=7,
        )
        token_service = TokenService(_config=config)
    if outbox is None:
        outbox = AsyncMock()
    if device_auth_repo is None:
        device_auth_repo = AsyncMock()
    if admin_orcids is None:
        admin_orcids = set()

    return AuthService(
        _user_repo=user_repo,
        _linked_account_repo=linked_account_repo,
        _refresh_token_repo=refresh_token_repo,
        _role_repo=role_repo,
        _device_auth_repo=device_auth_repo,
        _token_service=token_service,
        _outbox=outbox,
        _base_role=base_role,
        _admin_orcids=admin_orcids,
    )


def make_identity_provider(
    external_id: str = "0000-0001-2345-6789",
    display_name: str = "Jane Doe",
) -> MagicMock:
    """Create a mock identity provider."""
    provider = MagicMock()
    provider.provider_name = "orcid"

    identity_info = IdentityInfo(
        provider="orcid",
        external_id=external_id,
        display_name=display_name,
        email=None,
        raw_data={"name": display_name, "orcid": external_id},
    )

    provider.exchange_code = AsyncMock(return_value=identity_info)
    provider.get_authorization_url = MagicMock(return_value="https://orcid.org/oauth/authorize?...")
    return provider


class TestAdminBootstrapping:
    """T014 — Admin ORCiD in list gets SUPERADMIN on first login."""

    @pytest.mark.asyncio
    async def test_admin_orcid_gets_superadmin_on_first_login(self):
        """ORCiD in admin list → SUPERADMIN on first login."""
        linked_account_repo = AsyncMock()
        linked_account_repo.get_by_provider_and_external_id.return_value = None
        role_repo = AsyncMock()
        refresh_token_repo = AsyncMock()

        admin_orcid = "0000-0001-2345-6789"
        service = make_auth_service(
            linked_account_repo=linked_account_repo,
            refresh_token_repo=refresh_token_repo,
            role_repo=role_repo,
            base_role=Role.DEPOSITOR,
            admin_orcids={admin_orcid},
        )
        provider = make_identity_provider(external_id=admin_orcid)

        await service.complete_oauth(
            provider=provider,
            code="auth-code",
            redirect_uri="http://localhost/callback",
        )

        role_repo.save.assert_called_once()
        saved_assignment = role_repo.save.call_args[0][0]
        assert saved_assignment.role == Role.SUPERADMIN

    @pytest.mark.asyncio
    async def test_non_admin_orcid_gets_base_role(self):
        """ORCiD NOT in admin list → base_role (DEPOSITOR)."""
        linked_account_repo = AsyncMock()
        linked_account_repo.get_by_provider_and_external_id.return_value = None
        role_repo = AsyncMock()
        refresh_token_repo = AsyncMock()

        service = make_auth_service(
            linked_account_repo=linked_account_repo,
            refresh_token_repo=refresh_token_repo,
            role_repo=role_repo,
            base_role=Role.DEPOSITOR,
            admin_orcids={"0000-0099-9999-9999"},  # Different ORCiD
        )
        provider = make_identity_provider(external_id="0000-0001-2345-6789")

        await service.complete_oauth(
            provider=provider,
            code="auth-code",
            redirect_uri="http://localhost/callback",
        )

        role_repo.save.assert_called_once()
        saved_assignment = role_repo.save.call_args[0][0]
        assert saved_assignment.role == Role.DEPOSITOR

    @pytest.mark.asyncio
    async def test_empty_admins_set_gives_base_role(self):
        """Empty admins set → base_role for all."""
        linked_account_repo = AsyncMock()
        linked_account_repo.get_by_provider_and_external_id.return_value = None
        role_repo = AsyncMock()
        refresh_token_repo = AsyncMock()

        service = make_auth_service(
            linked_account_repo=linked_account_repo,
            refresh_token_repo=refresh_token_repo,
            role_repo=role_repo,
            base_role=Role.DEPOSITOR,
            admin_orcids=set(),
        )
        provider = make_identity_provider()

        await service.complete_oauth(
            provider=provider,
            code="auth-code",
            redirect_uri="http://localhost/callback",
        )

        role_repo.save.assert_called_once()
        saved_assignment = role_repo.save.call_args[0][0]
        assert saved_assignment.role == Role.DEPOSITOR

    @pytest.mark.asyncio
    async def test_existing_user_no_role_change(self):
        """Existing user with matching ORCiD → no role change on login."""
        from datetime import UTC, datetime
        from uuid import uuid4

        from osa.domain.auth.model.linked_account import LinkedAccount
        from osa.domain.auth.model.user import User
        from osa.domain.auth.model.value import IdentityId, UserId

        admin_orcid = "0000-0001-2345-6789"
        existing_user = User(
            id=UserId(uuid4()),
            display_name="Existing Admin",
            created_at=datetime.now(UTC),
            updated_at=None,
        )
        existing_linked_account = LinkedAccount(
            id=IdentityId(uuid4()),
            user_id=existing_user.id,
            provider="orcid",
            external_id=admin_orcid,
            metadata=None,
            created_at=datetime.now(UTC),
        )

        user_repo = AsyncMock()
        user_repo.get.return_value = existing_user
        linked_account_repo = AsyncMock()
        linked_account_repo.get_by_provider_and_external_id.return_value = existing_linked_account
        role_repo = AsyncMock()
        refresh_token_repo = AsyncMock()

        service = make_auth_service(
            user_repo=user_repo,
            linked_account_repo=linked_account_repo,
            refresh_token_repo=refresh_token_repo,
            role_repo=role_repo,
            base_role=Role.DEPOSITOR,
            admin_orcids={admin_orcid},
        )
        provider = make_identity_provider(external_id=admin_orcid)

        await service.complete_oauth(
            provider=provider,
            code="auth-code",
            redirect_uri="http://localhost/callback",
        )

        # Existing user — no new role assignment
        role_repo.save.assert_not_called()

    @pytest.mark.asyncio
    async def test_admin_gets_only_superadmin(self):
        """Admin gets only SUPERADMIN (single RoleAssignment, not SUPERADMIN + DEPOSITOR)."""
        linked_account_repo = AsyncMock()
        linked_account_repo.get_by_provider_and_external_id.return_value = None
        role_repo = AsyncMock()
        refresh_token_repo = AsyncMock()

        admin_orcid = "0000-0001-2345-6789"
        service = make_auth_service(
            linked_account_repo=linked_account_repo,
            refresh_token_repo=refresh_token_repo,
            role_repo=role_repo,
            base_role=Role.DEPOSITOR,
            admin_orcids={admin_orcid},
        )
        provider = make_identity_provider(external_id=admin_orcid)

        await service.complete_oauth(
            provider=provider,
            code="auth-code",
            redirect_uri="http://localhost/callback",
        )

        # Exactly one role assignment (SUPERADMIN only, not SUPERADMIN + DEPOSITOR)
        assert role_repo.save.call_count == 1
        saved_assignment = role_repo.save.call_args[0][0]
        assert saved_assignment.role == Role.SUPERADMIN
