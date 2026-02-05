"""ORCiD identity provider adapter."""

import logging
from urllib.parse import urlencode

import httpx

from osa.config import OrcidConfig
from osa.domain.auth.port.identity_provider import IdentityInfo, IdentityProvider
from osa.domain.shared.error import ExternalServiceError

logger = logging.getLogger(__name__)


class OrcidIdentityProvider(IdentityProvider):
    """IdentityProvider implementation for ORCiD OAuth."""

    def __init__(self, config: OrcidConfig, http_client: httpx.AsyncClient) -> None:
        self._config = config
        self._http = http_client

    @property
    def provider_name(self) -> str:
        return "orcid"

    def get_authorization_url(self, state: str, redirect_uri: str) -> str:
        """Generate ORCiD authorization URL."""
        params = {
            "client_id": self._config.client_id,
            "response_type": "code",
            "scope": "/authenticate",
            "redirect_uri": redirect_uri,
            "state": state,
        }
        return f"{self._config.base_url}/oauth/authorize?{urlencode(params)}"

    async def exchange_code(
        self,
        code: str,
        redirect_uri: str,
    ) -> IdentityInfo:
        """Exchange authorization code for identity information."""
        token_url = f"{self._config.base_url}/oauth/token"

        data = {
            "client_id": self._config.client_id,
            "client_secret": self._config.client_secret,
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
        }

        try:
            response = await self._http.post(
                token_url,
                data=data,
                headers={"Accept": "application/json"},
            )

            if response.status_code != 200:
                logger.error(
                    "ORCiD token exchange failed: status=%d, body=%s",
                    response.status_code,
                    response.text,
                )
                raise ExternalServiceError(
                    f"ORCiD token exchange failed: {response.status_code}",
                    code="idp_unavailable",
                )

            token_data = response.json()

        except httpx.RequestError as e:
            logger.exception("ORCiD request failed: %s", e)
            raise ExternalServiceError(
                "Failed to connect to ORCiD",
                code="idp_unavailable",
            ) from e

        # ORCiD returns user info directly in token response
        # {
        #   "access_token": "...",
        #   "token_type": "bearer",
        #   "scope": "/authenticate",
        #   "name": "Jane Doe",
        #   "orcid": "0000-0001-2345-6789"
        # }
        orcid_id = token_data.get("orcid")
        if not orcid_id:
            raise ExternalServiceError(
                "ORCiD response missing orcid field",
                code="oauth_error",
            )

        return IdentityInfo(
            provider="orcid",
            external_id=orcid_id,
            display_name=token_data.get("name"),
            email=None,  # ORCiD doesn't return email in basic auth
            raw_data=token_data,
        )
