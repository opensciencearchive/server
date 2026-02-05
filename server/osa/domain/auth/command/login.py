"""Login commands for OAuth authentication flow."""

import secrets
from dataclasses import dataclass

from osa.domain.auth.port.identity_provider import IdentityProvider
from osa.domain.auth.service.auth import AuthService
from osa.domain.shared.command import Command, CommandHandler, Result


class InitiateLogin(Command):
    """Command to start OAuth login flow."""

    redirect_uri: str  # Where to redirect after login
    provider: str = "orcid"


class InitiateLoginResult(Result):
    """Result containing authorization URL and state."""

    authorization_url: str
    state: str  # CSRF token - caller should store this for validation


@dataclass
class InitiateLoginHandler(CommandHandler[InitiateLogin, InitiateLoginResult]):
    """Handler for InitiateLogin command."""

    identity_provider: IdentityProvider

    async def run(self, cmd: InitiateLogin) -> InitiateLoginResult:
        """Generate authorization URL for OAuth login."""
        # Generate CSRF state token
        state = secrets.token_urlsafe(32)

        # Get authorization URL from identity provider
        authorization_url = self.identity_provider.get_authorization_url(
            state=state,
            redirect_uri=cmd.redirect_uri,
        )

        return InitiateLoginResult(
            authorization_url=authorization_url,
            state=state,
        )


class CompleteOAuth(Command):
    """Command to complete OAuth flow with authorization code."""

    code: str
    state: str
    redirect_uri: str


class CompleteOAuthResult(Result):
    """Result containing user info and tokens."""

    user_id: str
    display_name: str | None
    orcid_id: str
    access_token: str
    refresh_token: str
    expires_in: int  # Seconds until access token expires


@dataclass
class CompleteOAuthHandler(CommandHandler[CompleteOAuth, CompleteOAuthResult]):
    """Handler for CompleteOAuth command."""

    auth_service: AuthService
    identity_provider: IdentityProvider
    token_service_expire_seconds: int

    async def run(self, cmd: CompleteOAuth) -> CompleteOAuthResult:
        """Exchange authorization code for tokens and create/update user."""
        # Note: State validation should be done by the caller (route handler)
        # before invoking this command

        user, identity, access_token, refresh_token = await self.auth_service.complete_oauth(
            provider=self.identity_provider,
            code=cmd.code,
            redirect_uri=cmd.redirect_uri,
        )

        return CompleteOAuthResult(
            user_id=str(user.id),
            display_name=user.display_name,
            orcid_id=identity.external_id,
            access_token=access_token,
            refresh_token=refresh_token,
            expires_in=self.token_service_expire_seconds,
        )
