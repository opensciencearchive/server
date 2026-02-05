"""DI provider for auth domain."""

from dishka import provide

from osa.config import Config
from osa.domain.auth.port.repository import (
    IdentityRepository,
    RefreshTokenRepository,
    UserRepository,
)
from osa.domain.auth.service.auth import AuthService
from osa.domain.auth.service.token import TokenService
from osa.domain.shared.outbox import Outbox
from osa.util.di.base import Provider
from osa.util.di.scope import Scope


class AuthProvider(Provider):
    """DI provider for auth domain services and handlers."""

    @provide(scope=Scope.UOW)
    def get_token_service(self, config: Config) -> TokenService:
        """Provide TokenService."""
        return TokenService(_config=config.auth.jwt)

    @provide(scope=Scope.UOW)
    def get_auth_service(
        self,
        user_repo: UserRepository,
        identity_repo: IdentityRepository,
        refresh_token_repo: RefreshTokenRepository,
        token_service: TokenService,
        outbox: Outbox,
    ) -> AuthService:
        """Provide AuthService."""
        return AuthService(
            _user_repo=user_repo,
            _identity_repo=identity_repo,
            _refresh_token_repo=refresh_token_repo,
            _token_service=token_service,
            _outbox=outbox,
        )
