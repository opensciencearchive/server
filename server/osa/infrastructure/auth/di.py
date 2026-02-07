"""DI provider for auth infrastructure."""

import httpx
from dishka import provide

from osa.config import Config
from osa.domain.auth.port.identity_provider import IdentityProvider
from osa.domain.auth.port.provider_registry import ProviderRegistry
from osa.domain.auth.port.repository import (
    IdentityRepository,
    RefreshTokenRepository,
    UserRepository,
)
from osa.domain.auth.port.role_repository import RoleAssignmentRepository
from osa.infrastructure.auth.orcid import OrcidIdentityProvider
from osa.infrastructure.auth.provider_registry import InMemoryProviderRegistry
from osa.infrastructure.auth.role_repository import PostgresRoleAssignmentRepository
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
    role_assignment_repo = provide(
        PostgresRoleAssignmentRepository,
        scope=Scope.UOW,
        provides=RoleAssignmentRepository,
    )

    @provide(scope=Scope.APP)
    def get_auth_http_client(self) -> httpx.AsyncClient:
        """Shared HTTP client for auth operations (connection pooling)."""
        return httpx.AsyncClient(timeout=_HTTP_TIMEOUT)

    @provide(scope=Scope.APP)
    def get_provider_registry(
        self, config: Config, http_client: httpx.AsyncClient
    ) -> ProviderRegistry:
        """Provide ProviderRegistry with configured identity providers."""
        providers: dict[str, IdentityProvider] = {}

        # Register ORCID if configured
        if config.auth.orcid.client_id:
            providers["orcid"] = OrcidIdentityProvider(
                config=config.auth.orcid, http_client=http_client
            )

        return InMemoryProviderRegistry(providers)
