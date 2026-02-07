"""Token commands for refresh and logout operations."""

from dataclasses import dataclass
from uuid import uuid4

from osa.domain.auth.event import UserLoggedOut
from osa.domain.auth.service.auth import AuthService
from osa.domain.auth.service.token import TokenService
from osa.domain.shared.authorization.gate import public
from osa.domain.shared.command import Command, CommandHandler, Result
from osa.domain.shared.event import EventId
from osa.domain.shared.outbox import Outbox


class RefreshTokens(Command):
    """Command to refresh access token using refresh token."""

    refresh_token: str


class RefreshTokensResult(Result):
    """Result containing new tokens."""

    access_token: str
    refresh_token: str
    expires_in: int


@dataclass
class RefreshTokensHandler(CommandHandler[RefreshTokens, RefreshTokensResult]):
    """Handler for RefreshTokens command."""

    __auth__ = public()

    auth_service: AuthService
    token_service: TokenService

    async def run(self, cmd: RefreshTokens) -> RefreshTokensResult:
        """Refresh tokens using refresh token rotation."""
        _user, access_token, new_refresh_token = await self.auth_service.refresh_tokens(
            cmd.refresh_token
        )

        return RefreshTokensResult(
            access_token=access_token,
            refresh_token=new_refresh_token,
            expires_in=self.token_service.access_token_expire_seconds,
        )


class Logout(Command):
    """Command to logout and revoke refresh token family."""

    refresh_token: str


class LogoutResult(Result):
    """Result for logout operation."""

    success: bool


@dataclass
class LogoutHandler(CommandHandler[Logout, LogoutResult]):
    """Handler for Logout command."""

    __auth__ = public()

    auth_service: AuthService
    outbox: Outbox

    async def run(self, cmd: Logout) -> LogoutResult:
        """Logout by revoking refresh token family."""
        # Get user_id before revoking (for event emission)
        user_id = await self.auth_service.get_user_id_from_refresh_token(cmd.refresh_token)

        # Revoke tokens
        success = await self.auth_service.logout(cmd.refresh_token)

        # Emit UserLoggedOut event if we had a valid user
        if user_id is not None:
            await self.outbox.append(
                UserLoggedOut(
                    id=EventId(uuid4()),
                    user_id=str(user_id),
                )
            )

        return LogoutResult(success=success)
