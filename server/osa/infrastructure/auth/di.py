"""DI provider for auth infrastructure."""

import httpx
from dishka import provide

from osa.config import Config
from osa.domain.auth.port.identity_provider import IdentityProvider
from osa.domain.auth.port.repository import (
    IdentityRepository,
    RefreshTokenRepository,
    UserRepository,
)
from osa.infrastructure.auth.orcid import OrcidIdentityProvider
from osa.infrastructure.persistence.repository.auth import (
    PostgresIdentityRepository,
    PostgresRefreshTokenRepository,
    PostgresUserRepository,
)
from osa.util.di.base import Provider
from osa.util.di.scope import Scope

# HTTP client timeout configuration
_HTTP_TIMEOUT = httpx.Timeout(
    connect=5.0,  # Connection timeout
    read=10.0,  # Read timeout
    write=5.0,  # Write timeout
    pool=5.0,  # Pool timeout
)


class AuthInfraProvider(Provider):
    """DI provider for auth infrastructure adapters."""

    # Repository adapters
    user_repo = provide(
        PostgresUserRepository,
        scope=Scope.UOW,
        provides=UserRepository,
    )
    identity_repo = provide(
        PostgresIdentityRepository,
        scope=Scope.UOW,
        provides=IdentityRepository,
    )
    refresh_token_repo = provide(
        PostgresRefreshTokenRepository,
        scope=Scope.UOW,
        provides=RefreshTokenRepository,
    )

    @provide(scope=Scope.APP)
    def get_auth_http_client(self) -> httpx.AsyncClient:
        """Shared HTTP client for auth operations (connection pooling)."""
        return httpx.AsyncClient(timeout=_HTTP_TIMEOUT)

    @provide(scope=Scope.UOW)
    def get_orcid_provider(
        self, config: Config, http_client: httpx.AsyncClient
    ) -> IdentityProvider:
        """Provide OrcidIdentityProvider as the default IdentityProvider."""
        return OrcidIdentityProvider(config=config.auth.orcid, http_client=http_client)
