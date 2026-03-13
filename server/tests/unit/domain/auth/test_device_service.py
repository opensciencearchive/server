"""Unit tests for AuthService device flow methods and TokenService device_code state."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from osa.config import JwtConfig
from osa.domain.auth.model.device_authorization import (
    DeviceAuthorization,
    DeviceAuthorizationStatus,
)
from osa.domain.auth.model.linked_account import LinkedAccount
from osa.domain.auth.model.user import User
from osa.domain.auth.model.value import (
    DeviceAuthorizationId,
    IdentityId,
    UserCode,
    UserId,
)
from osa.domain.auth.service.auth import AuthService
from osa.domain.auth.service.token import TokenService
from osa.domain.shared.error import InvalidStateError


def make_token_service() -> TokenService:
    config = JwtConfig(
        secret="test-secret-key-256-bits-long-xx",
        algorithm="HS256",
        access_token_expire_minutes=60,
        refresh_token_expire_days=7,
    )
    return TokenService(_config=config)


def make_auth_service(
    device_auth_repo: AsyncMock | None = None,
    user_repo: AsyncMock | None = None,
    linked_account_repo: AsyncMock | None = None,
    refresh_token_repo: AsyncMock | None = None,
) -> AuthService:
    """Create an AuthService with mocked dependencies for device flow testing."""
    return AuthService(
        _user_repo=user_repo or AsyncMock(),
        _linked_account_repo=linked_account_repo or AsyncMock(),
        _refresh_token_repo=refresh_token_repo or AsyncMock(),
        _role_repo=AsyncMock(),
        _device_auth_repo=device_auth_repo or AsyncMock(),
        _token_service=make_token_service(),
        _outbox=AsyncMock(),
        _base_role=None,
    )


def make_device_auth(
    *,
    status: DeviceAuthorizationStatus = DeviceAuthorizationStatus.PENDING,
    user_id: UserId | None = None,
    expired: bool = False,
) -> DeviceAuthorization:
    """Create a DeviceAuthorization for testing."""
    now = datetime.now(UTC)
    expires_at = now - timedelta(minutes=1) if expired else now + timedelta(minutes=15)
    return DeviceAuthorization(
        id=DeviceAuthorizationId.generate(),
        device_code="a" * 64,
        user_code=UserCode("BCDF2347"),
        status=status,
        user_id=user_id,
        expires_at=expires_at,
        created_at=now,
    )


class TestTokenServiceDeviceCode:
    """Tests for TokenService state round-trip with device_code."""

    def test_state_without_device_code(self):
        """State without device_code should return None for device_code field."""
        svc = make_token_service()
        state = svc.create_oauth_state("https://example.com", "orcid")
        result = svc.verify_oauth_state(state)
        assert result is not None
        assert result.redirect_uri == "https://example.com"
        assert result.provider == "orcid"
        assert result.device_code is None

    def test_state_with_device_code(self):
        """State with device_code should round-trip the device_code."""
        svc = make_token_service()
        state = svc.create_oauth_state(
            "https://example.com",
            "orcid",
            device_code="abc123",
        )
        result = svc.verify_oauth_state(state)
        assert result is not None
        assert result.redirect_uri == "https://example.com"
        assert result.provider == "orcid"
        assert result.device_code == "abc123"


class TestCreateDeviceAuthorization:
    """Tests for AuthService.create_device_authorization."""

    @pytest.mark.asyncio
    async def test_creates_device_authorization(self):
        """create_device_authorization should persist and return a DeviceAuthorization."""
        device_auth_repo = AsyncMock()
        device_auth_repo.get_by_user_code.return_value = None

        service = make_auth_service(device_auth_repo=device_auth_repo)
        result = await service.create_device_authorization()

        assert isinstance(result, DeviceAuthorization)
        assert result.is_pending
        assert len(result.device_code) == 64
        device_auth_repo.save.assert_called_once()

    @pytest.mark.asyncio
    async def test_retries_on_user_code_collision(self):
        """create_device_authorization should retry on user_code collision (ConflictError from repo)."""
        from osa.domain.shared.error import ConflictError

        device_auth_repo = AsyncMock()
        # First save raises ConflictError (collision), second succeeds
        device_auth_repo.save.side_effect = [
            ConflictError("conflict", code="device_auth_conflict"),
            None,  # success
        ]

        service = make_auth_service(device_auth_repo=device_auth_repo)
        result = await service.create_device_authorization()

        assert isinstance(result, DeviceAuthorization)
        assert device_auth_repo.save.call_count == 2


class TestVerifyUserCode:
    """Tests for AuthService.verify_user_code."""

    @pytest.mark.asyncio
    async def test_returns_pending_authorization(self):
        """verify_user_code should return a pending, non-expired authorization."""
        device_auth = make_device_auth()
        device_auth_repo = AsyncMock()
        device_auth_repo.get_by_user_code.return_value = device_auth

        service = make_auth_service(device_auth_repo=device_auth_repo)
        result = await service.verify_user_code(UserCode("BCDF2347"))

        assert result is device_auth

    @pytest.mark.asyncio
    async def test_returns_none_for_unknown_code(self):
        """verify_user_code should return None if code not found."""
        device_auth_repo = AsyncMock()
        device_auth_repo.get_by_user_code.return_value = None

        service = make_auth_service(device_auth_repo=device_auth_repo)
        result = await service.verify_user_code(UserCode("BCDF2347"))

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_for_expired(self):
        """verify_user_code should return None if authorization is expired."""
        device_auth = make_device_auth(expired=True)
        device_auth_repo = AsyncMock()
        device_auth_repo.get_by_user_code.return_value = device_auth

        service = make_auth_service(device_auth_repo=device_auth_repo)
        result = await service.verify_user_code(UserCode("BCDF2347"))

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_for_non_pending(self):
        """verify_user_code should return None if not pending."""
        device_auth = make_device_auth(
            status=DeviceAuthorizationStatus.AUTHORIZED,
            user_id=UserId.generate(),
        )
        device_auth_repo = AsyncMock()
        device_auth_repo.get_by_user_code.return_value = device_auth

        service = make_auth_service(device_auth_repo=device_auth_repo)
        result = await service.verify_user_code(UserCode("BCDF2347"))

        assert result is None


class TestAuthorizeDevice:
    """Tests for AuthService.authorize_device."""

    @pytest.mark.asyncio
    async def test_authorizes_pending_device(self):
        """authorize_device should mark device as authorized with user_id."""
        device_auth = make_device_auth()
        device_auth_repo = AsyncMock()
        device_auth_repo.get_by_device_code.return_value = device_auth

        service = make_auth_service(device_auth_repo=device_auth_repo)
        user_id = UserId.generate()
        await service.authorize_device(device_auth.device_code, user_id)

        assert device_auth.is_authorized
        assert device_auth.user_id == user_id
        device_auth_repo.save.assert_called_once()

    @pytest.mark.asyncio
    async def test_raises_for_unknown_device_code(self):
        """authorize_device should raise if device code not found."""
        device_auth_repo = AsyncMock()
        device_auth_repo.get_by_device_code.return_value = None

        service = make_auth_service(device_auth_repo=device_auth_repo)

        with pytest.raises(InvalidStateError, match="not found"):
            await service.authorize_device("unknown", UserId.generate())

    @pytest.mark.asyncio
    async def test_raises_for_expired_device(self):
        """authorize_device should raise if device code is expired."""
        device_auth = make_device_auth(expired=True)
        device_auth_repo = AsyncMock()
        device_auth_repo.get_by_device_code.return_value = device_auth

        service = make_auth_service(device_auth_repo=device_auth_repo)

        with pytest.raises(InvalidStateError, match="expired"):
            await service.authorize_device(device_auth.device_code, UserId.generate())


class TestExchangeDeviceCode:
    """Tests for AuthService.exchange_device_code."""

    @pytest.mark.asyncio
    async def test_returns_tokens_for_authorized(self):
        """exchange_device_code should return tokens via atomic consume."""
        user_id = UserId.generate()
        # consume_if_authorized returns the entity already in CONSUMED status
        device_auth = make_device_auth(
            status=DeviceAuthorizationStatus.CONSUMED,
            user_id=user_id,
        )
        user = User(
            id=user_id,
            display_name="Test User",
            created_at=datetime.now(UTC),
            updated_at=None,
        )
        linked_account = LinkedAccount(
            id=IdentityId.generate(),
            user_id=user_id,
            provider="orcid",
            external_id="0000-0001-2345-6789",
            metadata=None,
            created_at=datetime.now(UTC),
        )

        device_auth_repo = AsyncMock()
        device_auth_repo.consume_if_authorized.return_value = device_auth
        user_repo = AsyncMock()
        user_repo.get.return_value = user
        linked_account_repo = AsyncMock()
        linked_account_repo.get_by_user_id.return_value = [linked_account]
        refresh_token_repo = AsyncMock()

        service = make_auth_service(
            device_auth_repo=device_auth_repo,
            user_repo=user_repo,
            linked_account_repo=linked_account_repo,
            refresh_token_repo=refresh_token_repo,
        )

        result = await service.exchange_device_code(device_auth.device_code)

        assert result is not None
        returned_user, access_token, refresh_token = result
        assert returned_user.id == user_id
        assert isinstance(access_token, str)
        assert isinstance(refresh_token, str)
        device_auth_repo.consume_if_authorized.assert_called_once_with(device_auth.device_code)
        refresh_token_repo.save.assert_called_once()

    @pytest.mark.asyncio
    async def test_returns_none_for_pending(self):
        """exchange_device_code should return None when still pending."""
        device_auth = make_device_auth()
        device_auth_repo = AsyncMock()
        device_auth_repo.consume_if_authorized.return_value = None
        device_auth_repo.get_by_device_code.return_value = device_auth

        service = make_auth_service(device_auth_repo=device_auth_repo)
        result = await service.exchange_device_code(device_auth.device_code)

        assert result is None

    @pytest.mark.asyncio
    async def test_raises_for_expired(self):
        """exchange_device_code should raise for expired device code."""
        device_auth = make_device_auth(expired=True)
        device_auth_repo = AsyncMock()
        device_auth_repo.consume_if_authorized.return_value = None
        device_auth_repo.get_by_device_code.return_value = device_auth

        service = make_auth_service(device_auth_repo=device_auth_repo)

        with pytest.raises(InvalidStateError) as exc_info:
            await service.exchange_device_code(device_auth.device_code)
        assert exc_info.value.code == "expired_token"

    @pytest.mark.asyncio
    async def test_raises_for_consumed(self):
        """exchange_device_code should raise for already-consumed code."""
        device_auth = make_device_auth(
            status=DeviceAuthorizationStatus.CONSUMED,
            user_id=UserId.generate(),
        )
        device_auth_repo = AsyncMock()
        device_auth_repo.consume_if_authorized.return_value = None
        device_auth_repo.get_by_device_code.return_value = device_auth

        service = make_auth_service(device_auth_repo=device_auth_repo)

        with pytest.raises(InvalidStateError, match="consumed"):
            await service.exchange_device_code(device_auth.device_code)

    @pytest.mark.asyncio
    async def test_raises_for_unknown(self):
        """exchange_device_code should raise for unknown device code."""
        device_auth_repo = AsyncMock()
        device_auth_repo.consume_if_authorized.return_value = None
        device_auth_repo.get_by_device_code.return_value = None

        service = make_auth_service(device_auth_repo=device_auth_repo)

        with pytest.raises(InvalidStateError, match="not found"):
            await service.exchange_device_code("unknown")

    @pytest.mark.asyncio
    async def test_concurrent_consume_only_one_wins(self):
        """When two callers race, only the one that gets consume_if_authorized succeeds."""
        user_id = UserId.generate()
        device_auth = make_device_auth(
            status=DeviceAuthorizationStatus.CONSUMED,
            user_id=user_id,
        )
        user = User(
            id=user_id,
            display_name="Test User",
            created_at=datetime.now(UTC),
            updated_at=None,
        )
        linked_account = LinkedAccount(
            id=IdentityId.generate(),
            user_id=user_id,
            provider="orcid",
            external_id="0000-0001-2345-6789",
            metadata=None,
            created_at=datetime.now(UTC),
        )

        # First caller wins, second gets None (already consumed)
        device_auth_repo = AsyncMock()
        device_auth_repo.consume_if_authorized.side_effect = [device_auth, None]
        # Second caller falls back to get_by_device_code and sees CONSUMED
        consumed_auth = make_device_auth(
            status=DeviceAuthorizationStatus.CONSUMED,
            user_id=user_id,
        )
        device_auth_repo.get_by_device_code.return_value = consumed_auth
        user_repo = AsyncMock()
        user_repo.get.return_value = user
        linked_account_repo = AsyncMock()
        linked_account_repo.get_by_user_id.return_value = [linked_account]
        refresh_token_repo = AsyncMock()

        service = make_auth_service(
            device_auth_repo=device_auth_repo,
            user_repo=user_repo,
            linked_account_repo=linked_account_repo,
            refresh_token_repo=refresh_token_repo,
        )

        # First call succeeds
        result1 = await service.exchange_device_code(device_auth.device_code)
        assert result1 is not None

        # Second call raises "consumed"
        with pytest.raises(InvalidStateError, match="consumed"):
            await service.exchange_device_code(device_auth.device_code)
