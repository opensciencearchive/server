"""Unit tests for auth command handlers."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from osa.config import JwtConfig
from osa.domain.auth.command.login import (
    CompleteOAuth,
    CompleteOAuthHandler,
    InitiateLogin,
    InitiateLoginHandler,
)
from osa.domain.auth.command.token import (
    Logout,
    LogoutHandler,
    RefreshTokens,
    RefreshTokensHandler,
)
from osa.domain.auth.event import UserAuthenticated, UserLoggedOut
from osa.domain.auth.model.linked_account import LinkedAccount
from osa.domain.auth.model.user import User
from osa.domain.auth.model.value import IdentityId, UserId
from osa.domain.auth.service.token import TokenService


def make_token_service() -> TokenService:
    """Create a TokenService with test config."""
    config = JwtConfig(
        secret="test-secret-key-256-bits-long-xx",
        algorithm="HS256",
        access_token_expire_minutes=60,
        refresh_token_expire_days=7,
    )
    return TokenService(_config=config)


def make_identity_provider() -> MagicMock:
    """Create a mock identity provider."""
    provider = MagicMock()
    provider.provider_name = "orcid"
    provider.get_authorization_url = MagicMock(
        return_value="https://orcid.org/oauth/authorize?state=xyz"
    )
    return provider


def make_provider_registry(identity_provider: MagicMock | None = None) -> MagicMock:
    """Create a mock provider registry."""
    if identity_provider is None:
        identity_provider = make_identity_provider()
    registry = MagicMock()
    registry.get.return_value = identity_provider
    registry.is_available.return_value = True
    registry.available_providers.return_value = ["orcid"]
    return registry


class TestInitiateLoginHandler:
    """Tests for InitiateLoginHandler."""

    @pytest.mark.asyncio
    async def test_run_returns_authorization_url(self):
        """Handler should return authorization URL from identity provider."""
        identity_provider = make_identity_provider()
        provider_registry = make_provider_registry(identity_provider)
        token_service = make_token_service()

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

        assert result.authorization_url == "https://orcid.org/oauth/authorize?state=xyz"
        identity_provider.get_authorization_url.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_creates_signed_state(self):
        """Handler should create signed state token with final redirect URI and provider."""
        identity_provider = make_identity_provider()
        provider_registry = make_provider_registry(identity_provider)
        token_service = make_token_service()

        handler = InitiateLoginHandler(
            provider_registry=provider_registry,
            token_service=token_service,
        )

        await handler.run(
            InitiateLogin(
                callback_url="http://localhost/callback",
                final_redirect_uri="http://localhost/dashboard",
                provider="orcid",
            )
        )

        # Verify state was passed to identity provider
        call_args = identity_provider.get_authorization_url.call_args
        state = call_args[1]["state"] if "state" in call_args[1] else call_args[0][0]

        # Verify state can be decoded to get back the redirect URI and provider
        result = token_service.verify_oauth_state(state)
        assert result is not None
        redirect_uri, provider = result
        assert redirect_uri == "http://localhost/dashboard"
        assert provider == "orcid"


class TestCompleteOAuthHandler:
    """Tests for CompleteOAuthHandler."""

    @pytest.mark.asyncio
    async def test_run_emits_user_authenticated_event(self):
        """Handler should emit UserAuthenticated event on successful OAuth."""
        user = User(
            id=UserId(uuid4()),
            display_name="Jane Doe",
            created_at=datetime.now(UTC),
            updated_at=None,
        )
        linked_account = LinkedAccount(
            id=IdentityId(uuid4()),
            user_id=user.id,
            provider="orcid",
            external_id="0000-0001-2345-6789",
            metadata=None,
            created_at=datetime.now(UTC),
        )

        auth_service = AsyncMock()
        auth_service.complete_oauth.return_value = (
            user,
            linked_account,
            "access-token",
            "refresh-token",
        )

        provider_registry = make_provider_registry()
        token_service = make_token_service()
        outbox = AsyncMock()

        handler = CompleteOAuthHandler(
            auth_service=auth_service,
            provider_registry=provider_registry,
            token_service=token_service,
            outbox=outbox,
        )

        await handler.run(
            CompleteOAuth(
                code="auth-code",
                callback_url="http://localhost/callback",
                provider="orcid",
            )
        )

        # Verify UserAuthenticated event was emitted
        outbox.append.assert_called_once()
        event = outbox.append.call_args[0][0]
        assert isinstance(event, UserAuthenticated)
        assert event.user_id == str(user.id)
        assert event.provider == "orcid"
        assert event.external_id == "0000-0001-2345-6789"

    @pytest.mark.asyncio
    async def test_run_returns_user_info_and_tokens(self):
        """Handler should return user info and tokens."""
        user = User(
            id=UserId(uuid4()),
            display_name="Jane Doe",
            created_at=datetime.now(UTC),
            updated_at=None,
        )
        linked_account = LinkedAccount(
            id=IdentityId(uuid4()),
            user_id=user.id,
            provider="orcid",
            external_id="0000-0001-2345-6789",
            metadata=None,
            created_at=datetime.now(UTC),
        )

        auth_service = AsyncMock()
        auth_service.complete_oauth.return_value = (
            user,
            linked_account,
            "access-token",
            "refresh-token",
        )

        provider_registry = make_provider_registry()
        token_service = make_token_service()
        outbox = AsyncMock()

        handler = CompleteOAuthHandler(
            auth_service=auth_service,
            provider_registry=provider_registry,
            token_service=token_service,
            outbox=outbox,
        )

        result = await handler.run(
            CompleteOAuth(
                code="auth-code",
                callback_url="http://localhost/callback",
                provider="orcid",
            )
        )

        assert result.user_id == str(user.id)
        assert result.display_name == "Jane Doe"
        assert result.provider == "orcid"
        assert result.external_id == "0000-0001-2345-6789"
        assert result.access_token == "access-token"
        assert result.refresh_token == "refresh-token"
        assert result.expires_in == 60 * 60  # 60 minutes in seconds


class TestRefreshTokensHandler:
    """Tests for RefreshTokensHandler."""

    @pytest.mark.asyncio
    async def test_run_returns_new_tokens(self):
        """Handler should return new tokens from auth service."""
        user = User(
            id=UserId(uuid4()),
            display_name="Test User",
            created_at=datetime.now(UTC),
            updated_at=None,
        )

        auth_service = AsyncMock()
        auth_service.refresh_tokens.return_value = (
            user,
            "new-access-token",
            "new-refresh-token",
        )

        token_service = make_token_service()

        handler = RefreshTokensHandler(
            auth_service=auth_service,
            token_service=token_service,
        )

        result = await handler.run(RefreshTokens(refresh_token="old-refresh-token"))

        assert result.access_token == "new-access-token"
        assert result.refresh_token == "new-refresh-token"
        assert result.expires_in == 60 * 60  # 60 minutes in seconds

        auth_service.refresh_tokens.assert_called_once_with("old-refresh-token")


class TestLogoutHandler:
    """Tests for LogoutHandler."""

    @pytest.mark.asyncio
    async def test_run_emits_user_logged_out_event(self):
        """Handler should emit UserLoggedOut event when user has valid token."""
        user_id = UserId(uuid4())

        auth_service = AsyncMock()
        auth_service.get_user_id_from_refresh_token.return_value = user_id
        auth_service.logout.return_value = True

        outbox = AsyncMock()

        handler = LogoutHandler(
            auth_service=auth_service,
            outbox=outbox,
        )

        result = await handler.run(Logout(refresh_token="refresh-token"))

        assert result.success is True

        # Verify UserLoggedOut event was emitted
        outbox.append.assert_called_once()
        event = outbox.append.call_args[0][0]
        assert isinstance(event, UserLoggedOut)
        assert event.user_id == str(user_id)

    @pytest.mark.asyncio
    async def test_run_does_not_emit_event_for_unknown_token(self):
        """Handler should not emit event if token is unknown."""
        auth_service = AsyncMock()
        auth_service.get_user_id_from_refresh_token.return_value = None
        auth_service.logout.return_value = True

        outbox = AsyncMock()

        handler = LogoutHandler(
            auth_service=auth_service,
            outbox=outbox,
        )

        result = await handler.run(Logout(refresh_token="unknown-token"))

        assert result.success is True

        # Should NOT emit event for unknown token
        outbox.append.assert_not_called()

    @pytest.mark.asyncio
    async def test_run_returns_success(self):
        """Handler should return success status from auth service."""
        auth_service = AsyncMock()
        auth_service.get_user_id_from_refresh_token.return_value = UserId(uuid4())
        auth_service.logout.return_value = True

        outbox = AsyncMock()

        handler = LogoutHandler(
            auth_service=auth_service,
            outbox=outbox,
        )

        result = await handler.run(Logout(refresh_token="refresh-token"))

        assert result.success is True
        auth_service.logout.assert_called_once_with("refresh-token")
