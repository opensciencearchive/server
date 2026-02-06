"""Provider registry port for the auth domain."""

from abc import abstractmethod
from typing import Protocol

from osa.domain.auth.port.identity_provider import IdentityProvider
from osa.domain.shared.port import Port


class ProviderRegistry(Port, Protocol):
    """Registry of available identity providers.

    Allows looking up identity providers by name and checking
    which providers are configured/available.
    """

    @abstractmethod
    def get(self, provider: str) -> IdentityProvider | None:
        """Get an identity provider by name.

        Args:
            provider: The provider name (e.g., "orcid", "google")

        Returns:
            The identity provider if available, None otherwise
        """
        ...

    @abstractmethod
    def available_providers(self) -> list[str]:
        """Get list of available provider names.

        Returns:
            List of provider names that can be used for authentication
        """
        ...

    def is_available(self, provider: str) -> bool:
        """Check if a provider is available.

        Args:
            provider: The provider name to check

        Returns:
            True if the provider is available
        """
        return provider in self.available_providers()
