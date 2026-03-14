"""Device flow commands for OAuth device authorization grant."""

from dataclasses import dataclass

from osa.domain.auth.model.device_authorization import DEVICE_POLL_INTERVAL
from osa.domain.auth.port.provider_registry import ProviderRegistry
from osa.domain.auth.service.auth import AuthService
from osa.domain.auth.service.token import TokenService
from osa.domain.shared.authorization.gate import public
from osa.domain.shared.command import Command, CommandHandler, Result
from osa.domain.shared.error import InvalidStateError, NotFoundError


# ============================================================================
# InitiateDeviceAuth — CLI calls this to start the device flow
# ============================================================================


class InitiateDeviceAuth(Command):
    """Command to initiate a device authorization flow."""

    verification_uri_base: str  # Base URL for the verification page


class InitiateDeviceAuthResult(Result):
    """Result containing device code, user code, and verification URI."""

    device_code: str
    user_code: str  # Display format (XXXX-XXXX)
    verification_uri: str
    expires_in: int
    interval: int


@dataclass
class InitiateDeviceAuthHandler(CommandHandler[InitiateDeviceAuth, InitiateDeviceAuthResult]):
    """Handler for InitiateDeviceAuth command."""

    __auth__ = public()

    auth_service: AuthService

    async def run(self, cmd: InitiateDeviceAuth) -> InitiateDeviceAuthResult:
        device_auth = await self.auth_service.create_device_authorization()
        verification_uri = f"{cmd.verification_uri_base}?code={device_auth.user_code.display}"

        return InitiateDeviceAuthResult(
            device_code=device_auth.device_code,
            user_code=device_auth.user_code.display,
            verification_uri=verification_uri,
            expires_in=int((device_auth.expires_at - device_auth.created_at).total_seconds()),
            interval=DEVICE_POLL_INTERVAL,
        )


# ============================================================================
# VerifyDeviceCode — user submits code on verification page
# ============================================================================


class VerifyDeviceCode(Command):
    """Command to verify a user code and generate OAuth authorization URL."""

    user_code: str
    callback_url: str
    provider: str


class VerifyDeviceCodeResult(Result):
    """Result containing the authorization URL to redirect to."""

    authorization_url: str


@dataclass
class VerifyDeviceCodeHandler(CommandHandler[VerifyDeviceCode, VerifyDeviceCodeResult]):
    """Handler for VerifyDeviceCode command."""

    __auth__ = public()

    auth_service: AuthService
    token_service: TokenService
    provider_registry: ProviderRegistry

    async def run(self, cmd: VerifyDeviceCode) -> VerifyDeviceCodeResult:
        from osa.domain.auth.model.value import UserCode

        try:
            normalized_code = UserCode(cmd.user_code)
        except ValueError as e:
            raise InvalidStateError(
                "Invalid code format",
                code="invalid_user_code",
            ) from e

        device_auth = await self.auth_service.verify_user_code(normalized_code)
        if device_auth is None:
            raise InvalidStateError(
                "Invalid or expired code",
                code="invalid_user_code",
            )

        identity_provider = self.provider_registry.get(cmd.provider)
        if identity_provider is None:
            raise NotFoundError(
                f"Provider not configured: {cmd.provider}",
                code="unknown_provider",
            )

        state = self.token_service.create_oauth_state(
            redirect_uri=cmd.callback_url,
            provider=cmd.provider,
            device_code=device_auth.device_code,
        )

        authorization_url = identity_provider.get_authorization_url(
            state=state,
            redirect_uri=cmd.callback_url,
        )

        return VerifyDeviceCodeResult(authorization_url=authorization_url)


# ============================================================================
# CompleteDeviceOAuth — callback completes device flow OAuth
# ============================================================================


class CompleteDeviceOAuth(Command):
    """Command to complete OAuth for device flow callback."""

    code: str
    callback_url: str
    provider: str
    device_code: str


class CompleteDeviceOAuthResult(Result):
    """Result indicating device OAuth completion."""

    pass


@dataclass
class CompleteDeviceOAuthHandler(CommandHandler[CompleteDeviceOAuth, CompleteDeviceOAuthResult]):
    """Handler for CompleteDeviceOAuth command."""

    __auth__ = public()

    auth_service: AuthService
    provider_registry: ProviderRegistry

    async def run(self, cmd: CompleteDeviceOAuth) -> CompleteDeviceOAuthResult:
        identity_provider = self.provider_registry.get(cmd.provider)
        if identity_provider is None:
            raise NotFoundError(
                f"Unknown identity provider: {cmd.provider}",
                code="unknown_provider",
            )

        await self.auth_service.complete_device_oauth(
            provider=identity_provider,
            code=cmd.code,
            redirect_uri=cmd.callback_url,
            device_code=cmd.device_code,
        )

        return CompleteDeviceOAuthResult()


# ============================================================================
# PollDeviceToken — CLI polls for token after user completes auth
# ============================================================================


DEVICE_CODE_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:device_code"


class PollDeviceToken(Command):
    """Command to poll for device authorization completion."""

    device_code: str
    grant_type: str


class PollDeviceTokenResult(Result):
    """Result containing tokens on success."""

    access_token: str
    refresh_token: str
    token_type: str = "Bearer"
    expires_in: int


@dataclass
class PollDeviceTokenHandler(CommandHandler[PollDeviceToken, PollDeviceTokenResult]):
    """Handler for PollDeviceToken command."""

    __auth__ = public()

    auth_service: AuthService
    token_service: TokenService

    async def run(self, cmd: PollDeviceToken) -> PollDeviceTokenResult:
        if cmd.grant_type != DEVICE_CODE_GRANT_TYPE:
            raise InvalidStateError(
                f"Invalid grant_type. Expected: {DEVICE_CODE_GRANT_TYPE}",
                code="unsupported_grant_type",
            )

        result = await self.auth_service.exchange_device_code(cmd.device_code)

        if result is None:
            raise InvalidStateError(
                "The user has not yet completed authorization.",
                code="authorization_pending",
            )

        return PollDeviceTokenResult(
            access_token=result.access_token,
            refresh_token=result.refresh_token,
            expires_in=self.token_service.access_token_expire_seconds,
        )
