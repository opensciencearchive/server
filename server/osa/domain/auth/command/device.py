"""Device flow commands for OAuth device authorization grant."""

from dataclasses import dataclass

from osa.domain.auth.model.device_authorization import DEVICE_POLL_INTERVAL
from osa.domain.auth.service.auth import AuthService
from osa.domain.auth.service.token import TokenService
from osa.domain.shared.authorization.gate import public
from osa.domain.shared.command import Command, CommandHandler, Result


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
        from osa.domain.shared.error import InvalidStateError

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

        user, access_token, refresh_token = result
        return PollDeviceTokenResult(
            access_token=access_token,
            refresh_token=refresh_token,
            expires_in=self.token_service.access_token_expire_seconds,
        )
