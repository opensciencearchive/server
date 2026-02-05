"""Identity provider port for the auth domain."""

from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Protocol

from osa.domain.shared.port import Port


@dataclass(frozen=True)
class IdentityInfo:
    """Information returned by an identity provider after successful auth."""

    provider: str  # e.g., "orcid", "google", "saml"
    external_id: str  # Provider-specific user ID
    display_name: str | None
    email: str | None  # May not be available from all providers
    raw_data: dict[str, Any]  # Full provider response for extensibility


class IdentityProvider(Port, Protocol):
    """Port for external identity provider integrations.

    Implementations are adapters in infrastructure/ (e.g., OrcidIdentityProvider).
    """

    @property
    @abstractmethod
    def provider_name(self) -> str:
        """Unique identifier for this provider (e.g., 'orcid')."""
        ...

    @abstractmethod
    def get_authorization_url(self, state: str, redirect_uri: str) -> str:
        """Generate URL to redirect user for authentication.

        Args:
            state: CSRF protection token (random, stored in session)
            redirect_uri: Where the IdP should redirect after auth

        Returns:
            Full URL to redirect the user to
        """
        ...

    @abstractmethod
    async def exchange_code(
        self,
        code: str,
        redirect_uri: str,
    ) -> IdentityInfo:
        """Exchange authorization code for identity information.

        Args:
            code: Authorization code from IdP callback
            redirect_uri: Must match the redirect_uri used in authorization URL

        Returns:
            IdentityInfo with user details from the provider

        Raises:
            ExternalServiceError: If the IdP request fails
        """
        ...
