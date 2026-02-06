"""Login commands for OAuth authentication flow."""

from dataclasses import dataclass
from uuid import uuid4

from osa.domain.auth.event import UserAuthenticated
from osa.domain.auth.port.identity_provider import IdentityProvider
from osa.domain.auth.service.auth import AuthService
from osa.domain.auth.service.token import TokenService
from osa.domain.shared.command import Command, CommandHandler, Result
from osa.domain.shared.event import EventId
from osa.domain.shared.outbox import Outbox


class InitiateLogin(Command):
    """Command to start OAuth login flow."""

    callback_url: str  # OAuth callback URL (where IdP redirects after auth)
    final_redirect_uri: str  # Where to redirect user after OAuth completes
    provider: str = "orcid"


class InitiateLoginResult(Result):
    """Result containing authorization URL."""

    authorization_url: str


@dataclass
class InitiateLoginHandler(CommandHandler[InitiateLogin, InitiateLoginResult]):
    """Handler for InitiateLogin command."""

    identity_provider: IdentityProvider
    token_service: TokenService

    async def run(self, cmd: InitiateLogin) -> InitiateLoginResult:
        """Generate authorization URL for OAuth login."""
        # Create signed state token (includes redirect_uri, expiry, and nonce)
        state = self.token_service.create_oauth_state(cmd.final_redirect_uri)

        # Get authorization URL from identity provider
        authorization_url = self.identity_provider.get_authorization_url(
            state=state,
            redirect_uri=cmd.callback_url,
        )

        return InitiateLoginResult(authorization_url=authorization_url)


class CompleteOAuth(Command):
    """Command to complete OAuth flow with authorization code."""

    code: str
    callback_url: str  # Must match the one used in authorization


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
    token_service: TokenService
    outbox: Outbox

    async def run(self, cmd: CompleteOAuth) -> CompleteOAuthResult:
        """Exchange authorization code for tokens and create/update user."""
        user, identity, access_token, refresh_token = await self.auth_service.complete_oauth(
            provider=self.identity_provider,
            code=cmd.code,
            redirect_uri=cmd.callback_url,
        )

        # Emit UserAuthenticated event
        await self.outbox.append(
            UserAuthenticated(
                id=EventId(uuid4()),
                user_id=str(user.id),
                provider=identity.provider,
                orcid_id=identity.external_id,
            )
        )

        return CompleteOAuthResult(
            user_id=str(user.id),
            display_name=user.display_name,
            orcid_id=identity.external_id,
            access_token=access_token,
            refresh_token=refresh_token,
            expires_in=self.token_service.access_token_expire_seconds,
        )
