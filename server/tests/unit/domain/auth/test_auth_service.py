"""Unit tests for AuthService."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from osa.config import JwtConfig
from osa.domain.auth.model.identity import Identity
from osa.domain.auth.model.token import RefreshToken
from osa.domain.auth.model.user import User
from osa.domain.auth.model.value import IdentityId, RefreshTokenId, TokenFamilyId, UserId
from osa.domain.auth.port.identity_provider import IdentityInfo
from osa.domain.auth.service.auth import AuthService
from osa.domain.auth.service.token import TokenService
from osa.domain.shared.error import InvalidStateError


def make_auth_service(
    user_repo: AsyncMock | None = None,
    identity_repo: AsyncMock | None = None,
    refresh_token_repo: AsyncMock | None = None,
    token_service: TokenService | None = None,
    outbox: AsyncMock | None = None,
) -> AuthService:
    """Create an AuthService with mocked dependencies."""
    if user_repo is None:
        user_repo = AsyncMock()
    if identity_repo is None:
        identity_repo = AsyncMock()
    if refresh_token_repo is None:
        refresh_token_repo = AsyncMock()
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

    return AuthService(
        _user_repo=user_repo,
        _identity_repo=identity_repo,
        _refresh_token_repo=refresh_token_repo,
        _token_service=token_service,
        _outbox=outbox,
    )


def make_identity_provider(identity_info: IdentityInfo | None = None) -> MagicMock:
    """Create a mock identity provider."""
    provider = MagicMock()
    provider.provider_name = "orcid"

    if identity_info is None:
        identity_info = IdentityInfo(
            provider="orcid",
            external_id="0000-0001-2345-6789",
            display_name="Jane Doe",
            email=None,
            raw_data={"name": "Jane Doe", "orcid": "0000-0001-2345-6789"},
        )

    provider.exchange_code = AsyncMock(return_value=identity_info)
    provider.get_authorization_url = MagicMock(return_value="https://orcid.org/oauth/authorize?...")
    return provider


class TestAuthServiceInitiateLogin:
    """Tests for AuthService.initiate_login."""

    @pytest.mark.asyncio
    async def test_initiate_login_returns_authorization_url(self):
        """initiate_login should return the provider's authorization URL."""
        service = make_auth_service()
        provider = make_identity_provider()
        provider.get_authorization_url.return_value = "https://orcid.org/oauth?state=abc"

        url = await service.initiate_login(
            provider=provider,
            state="test-state",
            redirect_uri="http://localhost/callback",
        )

        assert url == "https://orcid.org/oauth?state=abc"
        provider.get_authorization_url.assert_called_once_with(
            "test-state", "http://localhost/callback"
        )


class TestAuthServiceCompleteOAuth:
    """Tests for AuthService.complete_oauth."""

    @pytest.mark.asyncio
    async def test_complete_oauth_creates_new_user(self):
        """complete_oauth should create user and identity for new user."""
        user_repo = AsyncMock()
        user_repo.get.return_value = None  # No existing user

        identity_repo = AsyncMock()
        identity_repo.get_by_provider_and_external_id.return_value = None  # No existing identity

        refresh_token_repo = AsyncMock()

        service = make_auth_service(
            user_repo=user_repo,
            identity_repo=identity_repo,
            refresh_token_repo=refresh_token_repo,
        )
        provider = make_identity_provider()

        user, identity, access_token, refresh_token = await service.complete_oauth(
            provider=provider,
            code="auth-code",
            redirect_uri="http://localhost/callback",
        )

        # Should create user and identity
        user_repo.save.assert_called_once()
        identity_repo.save.assert_called_once()
        refresh_token_repo.save.assert_called_once()

        # Should return valid data
        assert user.display_name == "Jane Doe"
        assert identity.provider == "orcid"
        assert identity.external_id == "0000-0001-2345-6789"
        assert isinstance(access_token, str)
        assert isinstance(refresh_token, str)

    @pytest.mark.asyncio
    async def test_complete_oauth_returns_existing_user(self):
        """complete_oauth should return existing user if identity exists."""
        existing_user = User(
            id=UserId(uuid4()),
            display_name="Existing User",
            created_at=datetime.now(UTC),
            updated_at=None,
        )
        existing_identity = Identity(
            id=IdentityId(uuid4()),
            user_id=existing_user.id,
            provider="orcid",
            external_id="0000-0001-2345-6789",
            metadata=None,
            created_at=datetime.now(UTC),
        )

        user_repo = AsyncMock()
        user_repo.get.return_value = existing_user

        identity_repo = AsyncMock()
        identity_repo.get_by_provider_and_external_id.return_value = existing_identity

        refresh_token_repo = AsyncMock()

        service = make_auth_service(
            user_repo=user_repo,
            identity_repo=identity_repo,
            refresh_token_repo=refresh_token_repo,
        )
        provider = make_identity_provider()

        user, identity, _, _ = await service.complete_oauth(
            provider=provider,
            code="auth-code",
            redirect_uri="http://localhost/callback",
        )

        # Should NOT create new user/identity
        user_repo.save.assert_not_called()
        identity_repo.save.assert_not_called()

        # Should return existing user
        assert user.id == existing_user.id
        assert identity.id == existing_identity.id


class TestAuthServiceRefreshTokens:
    """Tests for AuthService.refresh_tokens."""

    @pytest.mark.asyncio
    async def test_refresh_tokens_issues_new_tokens(self):
        """refresh_tokens should issue new access and refresh tokens."""
        user = User(
            id=UserId(uuid4()),
            display_name="Test User",
            created_at=datetime.now(UTC),
            updated_at=None,
        )
        identity = Identity(
            id=IdentityId(uuid4()),
            user_id=user.id,
            provider="orcid",
            external_id="0000-0001-2345-6789",
            metadata=None,
            created_at=datetime.now(UTC),
        )
        old_token = RefreshToken(
            id=RefreshTokenId(uuid4()),
            user_id=user.id,
            token_hash="old-hash",
            family_id=TokenFamilyId(uuid4()),
            expires_at=datetime.now(UTC) + timedelta(days=7),
            created_at=datetime.now(UTC),
            revoked_at=None,
        )

        user_repo = AsyncMock()
        user_repo.get.return_value = user

        identity_repo = AsyncMock()
        identity_repo.get_by_user_id.return_value = [identity]

        refresh_token_repo = AsyncMock()
        refresh_token_repo.get_by_token_hash.return_value = old_token

        service = make_auth_service(
            user_repo=user_repo,
            identity_repo=identity_repo,
            refresh_token_repo=refresh_token_repo,
        )

        returned_user, access_token, new_refresh_token = await service.refresh_tokens(
            "raw-refresh-token"
        )

        # Should save new refresh token
        assert refresh_token_repo.save.call_count == 2  # Once for revoking old, once for new

        # Should return valid data
        assert returned_user.id == user.id
        assert isinstance(access_token, str)
        assert isinstance(new_refresh_token, str)

    @pytest.mark.asyncio
    async def test_refresh_tokens_revokes_old_token(self):
        """refresh_tokens should revoke the old refresh token."""
        user = User(
            id=UserId(uuid4()),
            display_name="Test User",
            created_at=datetime.now(UTC),
            updated_at=None,
        )
        identity = Identity(
            id=IdentityId(uuid4()),
            user_id=user.id,
            provider="orcid",
            external_id="0000-0001-2345-6789",
            metadata=None,
            created_at=datetime.now(UTC),
        )
        old_token = RefreshToken(
            id=RefreshTokenId(uuid4()),
            user_id=user.id,
            token_hash="old-hash",
            family_id=TokenFamilyId(uuid4()),
            expires_at=datetime.now(UTC) + timedelta(days=7),
            created_at=datetime.now(UTC),
            revoked_at=None,
        )

        user_repo = AsyncMock()
        user_repo.get.return_value = user

        identity_repo = AsyncMock()
        identity_repo.get_by_user_id.return_value = [identity]

        refresh_token_repo = AsyncMock()
        refresh_token_repo.get_by_token_hash.return_value = old_token

        service = make_auth_service(
            user_repo=user_repo,
            identity_repo=identity_repo,
            refresh_token_repo=refresh_token_repo,
        )

        await service.refresh_tokens("raw-refresh-token")

        # The old token should be revoked
        assert old_token.is_revoked is True

    @pytest.mark.asyncio
    async def test_refresh_tokens_rejects_invalid_token(self):
        """refresh_tokens should raise for unknown refresh token."""
        refresh_token_repo = AsyncMock()
        refresh_token_repo.get_by_token_hash.return_value = None

        service = make_auth_service(refresh_token_repo=refresh_token_repo)

        with pytest.raises(InvalidStateError) as exc_info:
            await service.refresh_tokens("invalid-token")

        assert exc_info.value.code == "invalid_refresh_token"

    @pytest.mark.asyncio
    async def test_refresh_tokens_detects_reuse_and_revokes_family(self):
        """refresh_tokens should revoke entire family if revoked token is reused."""
        user_id = UserId(uuid4())
        family_id = TokenFamilyId(uuid4())

        # Token that was already revoked (potential theft)
        revoked_token = RefreshToken(
            id=RefreshTokenId(uuid4()),
            user_id=user_id,
            token_hash="revoked-hash",
            family_id=family_id,
            expires_at=datetime.now(UTC) + timedelta(days=7),
            created_at=datetime.now(UTC),
            revoked_at=datetime.now(UTC) - timedelta(hours=1),  # Already revoked
        )

        refresh_token_repo = AsyncMock()
        refresh_token_repo.get_by_token_hash.return_value = revoked_token
        refresh_token_repo.revoke_family.return_value = 3  # 3 tokens revoked

        service = make_auth_service(refresh_token_repo=refresh_token_repo)

        with pytest.raises(InvalidStateError) as exc_info:
            await service.refresh_tokens("stolen-token")

        assert exc_info.value.code == "token_family_revoked"

        # Should revoke entire family
        refresh_token_repo.revoke_family.assert_called_once_with(family_id)

    @pytest.mark.asyncio
    async def test_refresh_tokens_rejects_expired_token(self):
        """refresh_tokens should raise for expired refresh token."""
        expired_token = RefreshToken(
            id=RefreshTokenId(uuid4()),
            user_id=UserId(uuid4()),
            token_hash="expired-hash",
            family_id=TokenFamilyId(uuid4()),
            expires_at=datetime.now(UTC) - timedelta(hours=1),  # Expired
            created_at=datetime.now(UTC) - timedelta(days=8),
            revoked_at=None,
        )

        refresh_token_repo = AsyncMock()
        refresh_token_repo.get_by_token_hash.return_value = expired_token

        service = make_auth_service(refresh_token_repo=refresh_token_repo)

        with pytest.raises(InvalidStateError) as exc_info:
            await service.refresh_tokens("expired-token")

        assert exc_info.value.code == "refresh_token_expired"


class TestAuthServiceLogout:
    """Tests for AuthService.logout."""

    @pytest.mark.asyncio
    async def test_logout_revokes_token_family(self):
        """logout should revoke the entire token family."""
        family_id = TokenFamilyId(uuid4())
        token = RefreshToken(
            id=RefreshTokenId(uuid4()),
            user_id=UserId(uuid4()),
            token_hash="token-hash",
            family_id=family_id,
            expires_at=datetime.now(UTC) + timedelta(days=7),
            created_at=datetime.now(UTC),
            revoked_at=None,
        )

        refresh_token_repo = AsyncMock()
        refresh_token_repo.get_by_token_hash.return_value = token
        refresh_token_repo.revoke_family.return_value = 1

        service = make_auth_service(refresh_token_repo=refresh_token_repo)

        result = await service.logout("raw-refresh-token")

        assert result is True
        refresh_token_repo.revoke_family.assert_called_once_with(family_id)

    @pytest.mark.asyncio
    async def test_logout_succeeds_for_unknown_token(self):
        """logout should succeed even if token is not found."""
        refresh_token_repo = AsyncMock()
        refresh_token_repo.get_by_token_hash.return_value = None

        service = make_auth_service(refresh_token_repo=refresh_token_repo)

        result = await service.logout("unknown-token")

        assert result is True
        refresh_token_repo.revoke_family.assert_not_called()
