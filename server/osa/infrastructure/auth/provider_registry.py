"""Provider registry implementation."""

from osa.domain.auth.port.identity_provider import IdentityProvider
from osa.domain.auth.port.provider_registry import ProviderRegistry


class InMemoryProviderRegistry(ProviderRegistry):
    """In-memory provider registry.

    Stores a mapping of provider names to their implementations.
    Providers are registered at application startup via DI.
    """

    def __init__(self, providers: dict[str, IdentityProvider] | None = None) -> None:
        """Initialize registry with optional initial providers.

        Args:
            providers: Optional dict mapping provider names to implementations
        """
        self._providers: dict[str, IdentityProvider] = providers or {}

    def get(self, provider: str) -> IdentityProvider | None:
        """Get an identity provider by name."""
        return self._providers.get(provider)

    def available_providers(self) -> list[str]:
        """Get list of available provider names."""
        return list(self._providers.keys())

    def register(self, name: str, provider: IdentityProvider) -> None:
        """Register a provider.

        Args:
            name: The provider name
            provider: The provider implementation
        """
        self._providers[name] = provider
