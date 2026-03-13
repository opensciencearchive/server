"""Unit tests for device flow command handlers."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from osa.config import JwtConfig
from osa.domain.auth.command.device import (
    DEVICE_CODE_GRANT_TYPE,
    InitiateDeviceAuth,
    InitiateDeviceAuthHandler,
    InitiateDeviceAuthResult,
    PollDeviceToken,
    PollDeviceTokenHandler,
    PollDeviceTokenResult,
)
from osa.domain.auth.model.device_authorization import (
    DEVICE_POLL_INTERVAL,
    DeviceAuthorization,
    DeviceAuthorizationStatus,
)
from osa.domain.auth.model.user import User
from osa.domain.auth.model.value import DeviceAuthorizationId, UserCode, UserId
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


def make_device_auth(
    *,
    status: DeviceAuthorizationStatus = DeviceAuthorizationStatus.PENDING,
    user_id: UserId | None = None,
) -> DeviceAuthorization:
    now = datetime.now(UTC)
    return DeviceAuthorization(
        id=DeviceAuthorizationId.generate(),
        device_code="a" * 64,
        user_code=UserCode("BCDF2347"),
        status=status,
        user_id=user_id,
        expires_at=now + timedelta(minutes=15),
        created_at=now,
    )


class TestInitiateDeviceAuthHandler:
    """Tests for InitiateDeviceAuthHandler."""

    @pytest.mark.asyncio
    async def test_returns_device_auth_result(self):
        """Handler should delegate to auth_service and return result."""
        device_auth = make_device_auth()
        auth_service = AsyncMock()
        auth_service.create_device_authorization.return_value = device_auth

        handler = InitiateDeviceAuthHandler(auth_service=auth_service)

        result = await handler.run(
            InitiateDeviceAuth(verification_uri_base="https://example.com/device/verify")
        )

        assert isinstance(result, InitiateDeviceAuthResult)
        assert result.device_code == device_auth.device_code
        assert result.user_code == device_auth.user_code.display
        assert result.interval == DEVICE_POLL_INTERVAL
        auth_service.create_device_authorization.assert_called_once()

    @pytest.mark.asyncio
    async def test_verification_uri_includes_user_code(self):
        """Handler should build verification URI with code query param."""
        device_auth = make_device_auth()
        auth_service = AsyncMock()
        auth_service.create_device_authorization.return_value = device_auth

        handler = InitiateDeviceAuthHandler(auth_service=auth_service)

        result = await handler.run(
            InitiateDeviceAuth(verification_uri_base="https://example.com/device/verify")
        )

        assert "code=" in result.verification_uri
        assert device_auth.user_code.display in result.verification_uri

    @pytest.mark.asyncio
    async def test_expires_in_matches_device_auth_ttl(self):
        """Handler should compute expires_in from entity timestamps."""
        device_auth = make_device_auth()
        auth_service = AsyncMock()
        auth_service.create_device_authorization.return_value = device_auth

        handler = InitiateDeviceAuthHandler(auth_service=auth_service)

        result = await handler.run(
            InitiateDeviceAuth(verification_uri_base="https://example.com/device/verify")
        )

        expected_seconds = int((device_auth.expires_at - device_auth.created_at).total_seconds())
        assert result.expires_in == expected_seconds


class TestPollDeviceTokenHandler:
    """Tests for PollDeviceTokenHandler."""

    @pytest.mark.asyncio
    async def test_returns_tokens_when_authorized(self):
        """Handler should return tokens when exchange succeeds."""
        user_id = UserId.generate()
        user = User(
            id=user_id,
            display_name="Test User",
            created_at=datetime.now(UTC),
            updated_at=None,
        )

        auth_service = AsyncMock()
        auth_service.exchange_device_code.return_value = (
            user,
            "access-token-123",
            "refresh-token-456",
        )
        token_service = make_token_service()

        handler = PollDeviceTokenHandler(
            auth_service=auth_service,
            token_service=token_service,
        )

        result = await handler.run(
            PollDeviceToken(
                device_code="a" * 64,
                grant_type=DEVICE_CODE_GRANT_TYPE,
            )
        )

        assert isinstance(result, PollDeviceTokenResult)
        assert result.access_token == "access-token-123"
        assert result.refresh_token == "refresh-token-456"
        assert result.token_type == "Bearer"
        assert result.expires_in == 60 * 60  # 60 min in seconds

    @pytest.mark.asyncio
    async def test_raises_authorization_pending_when_not_yet_authorized(self):
        """Handler should raise authorization_pending when exchange returns None."""
        auth_service = AsyncMock()
        auth_service.exchange_device_code.return_value = None
        token_service = make_token_service()

        handler = PollDeviceTokenHandler(
            auth_service=auth_service,
            token_service=token_service,
        )

        with pytest.raises(InvalidStateError) as exc_info:
            await handler.run(
                PollDeviceToken(
                    device_code="a" * 64,
                    grant_type=DEVICE_CODE_GRANT_TYPE,
                )
            )
        assert exc_info.value.code == "authorization_pending"

    @pytest.mark.asyncio
    async def test_rejects_invalid_grant_type(self):
        """Handler should reject non-device-code grant types."""
        auth_service = AsyncMock()
        token_service = make_token_service()

        handler = PollDeviceTokenHandler(
            auth_service=auth_service,
            token_service=token_service,
        )

        with pytest.raises(InvalidStateError) as exc_info:
            await handler.run(
                PollDeviceToken(
                    device_code="a" * 64,
                    grant_type="authorization_code",
                )
            )
        assert exc_info.value.code == "unsupported_grant_type"
        auth_service.exchange_device_code.assert_not_called()

    @pytest.mark.asyncio
    async def test_propagates_expired_token_error(self):
        """Handler should propagate expired_token from service."""
        auth_service = AsyncMock()
        auth_service.exchange_device_code.side_effect = InvalidStateError(
            "The device code has expired",
            code="expired_token",
        )
        token_service = make_token_service()

        handler = PollDeviceTokenHandler(
            auth_service=auth_service,
            token_service=token_service,
        )

        with pytest.raises(InvalidStateError) as exc_info:
            await handler.run(
                PollDeviceToken(
                    device_code="a" * 64,
                    grant_type=DEVICE_CODE_GRANT_TYPE,
                )
            )
        assert exc_info.value.code == "expired_token"
