"""Contract tests for device flow routes.

Tests the route-level behaviors through the command handlers and service methods
that the routes delegate to. Covers: POST /auth/device, GET /auth/device/verify,
POST /auth/device/verify, POST /auth/device/token, and the modified /auth/callback
with device_code in state.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from osa.config import JwtConfig
from osa.domain.auth.command.device import (
    DEVICE_CODE_GRANT_TYPE,
    InitiateDeviceAuth,
    InitiateDeviceAuthHandler,
    PollDeviceToken,
    PollDeviceTokenHandler,
)
from osa.domain.auth.model.device_authorization import (
    DEVICE_POLL_INTERVAL,
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


# ============================================================================
# T022: POST /auth/device — returns device_code, user_code, verification_uri
# ============================================================================


class TestInitiateDeviceFlow:
    """Contract: POST /auth/device returns device_code, user_code, verification_uri, expires_in, interval."""

    @pytest.mark.asyncio
    async def test_response_contains_all_required_fields(self):
        """Initiation response must include device_code, user_code, verification_uri, expires_in, interval."""
        device_auth = make_device_auth()
        auth_service = AsyncMock()
        auth_service.create_device_authorization.return_value = device_auth

        handler = InitiateDeviceAuthHandler(auth_service=auth_service)
        result = await handler.run(
            InitiateDeviceAuth(
                verification_uri_base="https://archive.example.com/api/v1/auth/device/verify"
            )
        )

        assert result.device_code == device_auth.device_code
        assert result.user_code == "BCDF-2347"  # Display format with hyphen
        assert "device/verify" in result.verification_uri
        assert result.expires_in > 0
        assert result.interval == DEVICE_POLL_INTERVAL

    @pytest.mark.asyncio
    async def test_verification_uri_pre_fills_code(self):
        """Verification URI should include the user code as a query parameter."""
        device_auth = make_device_auth()
        auth_service = AsyncMock()
        auth_service.create_device_authorization.return_value = device_auth

        handler = InitiateDeviceAuthHandler(auth_service=auth_service)
        result = await handler.run(
            InitiateDeviceAuth(
                verification_uri_base="https://example.com/api/v1/auth/device/verify"
            )
        )

        assert result.verification_uri.startswith(
            "https://example.com/api/v1/auth/device/verify?code="
        )


# ============================================================================
# T023: GET /auth/device/verify — HTML page, pre-fills code from query param
# ============================================================================


class TestDeviceVerifyPage:
    """Contract: GET /auth/device/verify returns HTML page with form and pre-filled code."""

    def test_verify_template_has_form_action(self):
        """The verify template should contain a form with POST action placeholder."""
        from pathlib import Path

        template_path = (
            Path(__file__).parents[6]
            / "osa"
            / "application"
            / "api"
            / "v1"
            / "templates"
            / "device"
            / "verify.html"
        )
        html = template_path.read_text()

        assert "{action_url}" in html
        assert 'method="POST"' in html
        assert 'name="user_code"' in html
        assert "{prefilled_code}" in html

    def test_verify_template_renders_with_prefilled_code(self):
        """The verify template should render with a pre-filled code value."""
        from pathlib import Path

        template_path = (
            Path(__file__).parents[6]
            / "osa"
            / "application"
            / "api"
            / "v1"
            / "templates"
            / "device"
            / "verify.html"
        )
        html = template_path.read_text()

        rendered = html.format(
            action_url="/api/v1/auth/device/verify",
            prefilled_code="BCDF-2347",
            error_html="",
        )

        assert 'value="BCDF-2347"' in rendered
        assert 'action="/api/v1/auth/device/verify"' in rendered

    def test_verify_template_renders_error(self):
        """The verify template should render error messages."""
        from pathlib import Path

        template_path = (
            Path(__file__).parents[6]
            / "osa"
            / "application"
            / "api"
            / "v1"
            / "templates"
            / "device"
            / "verify.html"
        )
        html = template_path.read_text()

        rendered = html.format(
            action_url="/api/v1/auth/device/verify",
            prefilled_code="XXXX",
            error_html='<p class="error">Invalid code.</p>',
        )

        assert "Invalid code." in rendered
        assert 'class="error"' in rendered


# ============================================================================
# T024: POST /auth/device/verify — valid code → redirect, invalid → error
# ============================================================================


class TestSubmitDeviceCode:
    """Contract: POST /auth/device/verify validates code and redirects to ORCID OAuth."""

    @pytest.mark.asyncio
    async def test_valid_code_looks_up_pending_device_auth(self):
        """Valid user code should look up a pending device authorization."""
        device_auth = make_device_auth()
        device_auth_repo = AsyncMock()
        device_auth_repo.get_by_user_code.return_value = device_auth

        service = make_auth_service(device_auth_repo=device_auth_repo)
        result = await service.verify_user_code(UserCode("BCDF2347"))

        assert result is not None
        assert result.device_code == device_auth.device_code

    @pytest.mark.asyncio
    async def test_invalid_code_returns_none(self):
        """Invalid user code should return None (route redirects with error)."""
        device_auth_repo = AsyncMock()
        device_auth_repo.get_by_user_code.return_value = None

        service = make_auth_service(device_auth_repo=device_auth_repo)
        result = await service.verify_user_code(UserCode("ZZZZ9999"))

        assert result is None

    @pytest.mark.asyncio
    async def test_expired_code_returns_none(self):
        """Expired device authorization should return None."""
        device_auth = make_device_auth(expired=True)
        device_auth_repo = AsyncMock()
        device_auth_repo.get_by_user_code.return_value = device_auth

        service = make_auth_service(device_auth_repo=device_auth_repo)
        result = await service.verify_user_code(UserCode("BCDF2347"))

        assert result is None

    def test_oauth_state_embeds_device_code(self):
        """OAuth state created for device flow should embed the device_code."""
        token_service = make_token_service()
        state = token_service.create_oauth_state(
            "https://example.com/callback",
            "orcid",
            device_code="abc123def456",
        )

        result = token_service.verify_oauth_state(state)
        assert result is not None
        assert result.device_code == "abc123def456"
        assert result.provider == "orcid"


# ============================================================================
# T025: POST /auth/device/token — pending, authorized, expired, bad grant_type
# ============================================================================


class TestPollDeviceToken:
    """Contract: POST /auth/device/token returns tokens or RFC 8628 error codes."""

    @pytest.mark.asyncio
    async def test_pending_returns_authorization_pending(self):
        """Polling a pending device code should raise authorization_pending."""
        auth_service = AsyncMock()
        auth_service.exchange_device_code.return_value = None
        token_service = make_token_service()

        handler = PollDeviceTokenHandler(auth_service=auth_service, token_service=token_service)

        with pytest.raises(InvalidStateError) as exc_info:
            await handler.run(
                PollDeviceToken(
                    device_code="a" * 64,
                    grant_type=DEVICE_CODE_GRANT_TYPE,
                )
            )
        assert exc_info.value.code == "authorization_pending"

    @pytest.mark.asyncio
    async def test_authorized_returns_tokens(self):
        """Polling an authorized device code should return tokens."""
        user = User(
            id=UserId.generate(),
            display_name="Test",
            created_at=datetime.now(UTC),
            updated_at=None,
        )
        auth_service = AsyncMock()
        auth_service.exchange_device_code.return_value = (
            user,
            "at-123",
            "rt-456",
        )
        token_service = make_token_service()

        handler = PollDeviceTokenHandler(auth_service=auth_service, token_service=token_service)

        result = await handler.run(
            PollDeviceToken(
                device_code="a" * 64,
                grant_type=DEVICE_CODE_GRANT_TYPE,
            )
        )

        assert result.access_token == "at-123"
        assert result.refresh_token == "rt-456"
        assert result.token_type == "Bearer"
        assert result.expires_in > 0

    @pytest.mark.asyncio
    async def test_expired_returns_expired_token_error(self):
        """Polling an expired device code should raise expired_token."""
        auth_service = AsyncMock()
        auth_service.exchange_device_code.side_effect = InvalidStateError(
            "expired", code="expired_token"
        )
        token_service = make_token_service()

        handler = PollDeviceTokenHandler(auth_service=auth_service, token_service=token_service)

        with pytest.raises(InvalidStateError) as exc_info:
            await handler.run(
                PollDeviceToken(
                    device_code="a" * 64,
                    grant_type=DEVICE_CODE_GRANT_TYPE,
                )
            )
        assert exc_info.value.code == "expired_token"

    @pytest.mark.asyncio
    async def test_invalid_grant_type_returns_unsupported(self):
        """Wrong grant_type should raise unsupported_grant_type without calling service."""
        auth_service = AsyncMock()
        token_service = make_token_service()

        handler = PollDeviceTokenHandler(auth_service=auth_service, token_service=token_service)

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
    async def test_consumed_returns_device_consumed_error(self):
        """Polling a consumed device code should raise error."""
        auth_service = AsyncMock()
        auth_service.exchange_device_code.side_effect = InvalidStateError(
            "consumed", code="device_consumed"
        )
        token_service = make_token_service()

        handler = PollDeviceTokenHandler(auth_service=auth_service, token_service=token_service)

        with pytest.raises(InvalidStateError) as exc_info:
            await handler.run(
                PollDeviceToken(
                    device_code="a" * 64,
                    grant_type=DEVICE_CODE_GRANT_TYPE,
                )
            )
        assert exc_info.value.code == "device_consumed"


# ============================================================================
# T026: /auth/callback with device_code in state — device flow completion
# ============================================================================


class TestCallbackDeviceFlow:
    """Contract: /auth/callback with device_code in state completes device flow."""

    @pytest.mark.asyncio
    async def test_device_callback_creates_user_and_authorizes_device(self):
        """When callback has device_code in state, it should find/create user
        then authorize the device (no tokens minted at callback time)."""
        user_id = UserId.generate()
        user = User(
            id=user_id,
            display_name="Researcher",
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
        device_auth = make_device_auth()
        device_auth_repo.get_by_device_code.return_value = device_auth

        linked_account_repo = AsyncMock()
        linked_account_repo.get_by_provider_and_external_id.return_value = linked_account

        user_repo = AsyncMock()
        user_repo.get.return_value = user

        service = make_auth_service(
            device_auth_repo=device_auth_repo,
            user_repo=user_repo,
            linked_account_repo=linked_account_repo,
        )

        # Simulate what the callback route does for device flow:
        # 1. find_or_create_user (no tokens)
        from osa.domain.auth.port.identity_provider import IdentityInfo

        identity_info = IdentityInfo(
            provider="orcid",
            external_id="0000-0001-2345-6789",
            display_name="Researcher",
            email=None,
            raw_data={},
        )
        found_user, _ = await service.find_or_create_user(identity_info)
        assert found_user.id == user_id

        # 2. authorize_device
        await service.authorize_device(device_auth.device_code, found_user.id)

        assert device_auth.is_authorized
        assert device_auth.user_id == user_id
        device_auth_repo.save.assert_called_once()

    def test_state_round_trip_with_device_code(self):
        """OAuth state with device_code should round-trip through create/verify."""
        token_service = make_token_service()
        device_code = "abc" * 20  # 60 char device code

        state = token_service.create_oauth_state(
            redirect_uri="https://example.com/callback",
            provider="orcid",
            device_code=device_code,
        )

        result = token_service.verify_oauth_state(state)
        assert result is not None
        assert result.device_code == device_code
        assert result.provider == "orcid"
        assert result.redirect_uri == "https://example.com/callback"

    def test_state_without_device_code_is_standard_flow(self):
        """OAuth state without device_code should have device_code=None."""
        token_service = make_token_service()

        state = token_service.create_oauth_state(
            redirect_uri="https://example.com/after",
            provider="orcid",
        )

        result = token_service.verify_oauth_state(state)
        assert result is not None
        assert result.device_code is None

    @pytest.mark.asyncio
    async def test_device_flow_tokens_minted_on_exchange_not_callback(self):
        """Tokens should be minted during exchange_device_code, not during callback.

        The callback only sets status to AUTHORIZED. The CLI polls POST /auth/device/token
        which calls exchange_device_code to mint tokens and mark as CONSUMED.
        """
        user_id = UserId.generate()
        user = User(
            id=user_id,
            display_name="Test",
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

        device_auth = make_device_auth()
        device_auth_repo = AsyncMock()
        device_auth_repo.get_by_device_code.return_value = device_auth
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

        # Step 1: authorize (callback) — no tokens minted
        await service.authorize_device(device_auth.device_code, user_id)
        assert device_auth.is_authorized
        refresh_token_repo.save.assert_not_called()

        # Step 2: exchange (poll) — tokens minted, marked consumed
        result = await service.exchange_device_code(device_auth.device_code)
        assert result is not None
        _, access_token, refresh_token = result
        assert isinstance(access_token, str)
        assert isinstance(refresh_token, str)
        assert device_auth.is_consumed
        refresh_token_repo.save.assert_called_once()
