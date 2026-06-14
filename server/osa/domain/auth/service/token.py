"""Token service for JWT creation and validation."""

import hashlib
import hmac
import json
import logging
import secrets
import time
from base64 import urlsafe_b64decode, urlsafe_b64encode
from datetime import UTC, datetime, timedelta
from typing import Any

import jwt

from osa.config import ExtraIssuerConfig, JwtConfig
from osa.domain.auth.model.value import OAuthStateData, ProviderIdentity, UserId
from osa.domain.shared.service import Service

logger = logging.getLogger(__name__)

# OAuth state validity period (5 minutes)
STATE_EXPIRY_SECONDS = 300


class TokenService(Service):
    """Service for JWT access token and refresh token operations.

    - Access tokens are JWTs (HS256) with user claims
    - Refresh tokens are opaque random strings, stored as hashes in the database
    - OAuth state tokens are signed payloads for CSRF protection
    """

    _config: JwtConfig
    # Optional second issuer for M2M tokens (#145, US5). None → single-issuer
    # behaviour, byte-identical to before.
    _extra_issuer: ExtraIssuerConfig | None = None

    @property
    def extra_issuer(self) -> ExtraIssuerConfig | None:
        """The configured M2M issuer, if any (read by identity resolution)."""
        return self._extra_issuer

    def create_access_token(
        self,
        user_id: UserId,
        identity: ProviderIdentity,
        additional_claims: dict[str, Any] | None = None,
    ) -> str:
        """Create a JWT access token.

        Args:
            user_id: The user's internal ID
            identity: The user's external identity (provider + external_id)
            additional_claims: Optional extra claims to include

        Returns:
            Encoded JWT string
        """
        now = datetime.now(UTC)
        expires_at = now + timedelta(minutes=self._config.access_token_expire_minutes)

        payload = {
            "sub": str(user_id),
            "provider": identity.provider,
            "external_id": identity.external_id,
            "aud": "authenticated",
            "iat": int(now.timestamp()),
            "exp": int(expires_at.timestamp()),
            "jti": secrets.token_hex(16),
        }

        if additional_claims:
            payload.update(additional_claims)

        return jwt.encode(
            payload,
            self._config.secret,
            algorithm=self._config.algorithm,
        )

    def validate_access_token(self, token: str) -> dict[str, Any]:
        """Validate and decode a JWT access token.

        Routes on the ``iss`` claim (#145, US5): when a second issuer is
        configured and the token's ``iss`` matches it, verify against that
        issuer's public key (asymmetric, verify-only) and audience. Otherwise —
        and always when no second issuer is configured — verify with the primary
        HS256 secret, byte-identical to the single-issuer path (SC-007).

        Args:
            token: The JWT string to validate

        Returns:
            Decoded payload dict

        Raises:
            jwt.InvalidTokenError: If token is invalid or expired
        """
        if self._extra_issuer is not None:
            # Read `iss` without verifying — routing only; the branch below
            # performs the real signature + audience verification. The algorithm
            # is pinned to EdDSA (Ed25519): the token header's `alg` is never
            # trusted, so a token can't downgrade to `none`/HS256.
            unverified = jwt.decode(token, options={"verify_signature": False})
            if unverified.get("iss") == self._extra_issuer.issuer:
                return jwt.decode(
                    token,
                    self._extra_issuer.public_key,
                    algorithms=["EdDSA"],
                    audience=self._extra_issuer.audience,
                    issuer=self._extra_issuer.issuer,
                )

        return jwt.decode(
            token,
            self._config.secret,
            algorithms=[self._config.algorithm],
            audience="authenticated",
        )

    def create_refresh_token(self) -> tuple[str, str]:
        """Create a new refresh token.

        Returns:
            Tuple of (raw_token, token_hash)
            - raw_token: Send to client
            - token_hash: Store in database
        """
        raw_token = secrets.token_urlsafe(32)
        token_hash = self.hash_token(raw_token)
        return raw_token, token_hash

    @staticmethod
    def hash_token(raw_token: str) -> str:
        """Create SHA256 hash of a token.

        Args:
            raw_token: The raw token string

        Returns:
            Hex-encoded SHA256 hash (64 characters)
        """
        return hashlib.sha256(raw_token.encode()).hexdigest()

    @property
    def access_token_expire_seconds(self) -> int:
        """Get access token expiry in seconds."""
        return self._config.access_token_expire_minutes * 60

    @property
    def refresh_token_expire_days(self) -> int:
        """Get refresh token expiry in days."""
        return self._config.refresh_token_expire_days

    def create_oauth_state(
        self,
        redirect_uri: str,
        provider: str,
        *,
        device_code: str | None = None,
    ) -> str:
        """Create a signed, self-verifying OAuth state token.

        The state contains: nonce, redirect_uri, provider, expiry timestamp,
        and optionally a device_code for the device authorization flow.

        Args:
            redirect_uri: The URI to redirect to after OAuth completes
            provider: The identity provider name (e.g., "orcid")
            device_code: Optional device code to embed (for device flow)

        Returns:
            URL-safe signed state token in format: payload.signature
        """
        payload: dict[str, Any] = {
            "nonce": secrets.token_urlsafe(16),
            "redirect_uri": redirect_uri,
            "provider": provider,
            "exp": int(time.time()) + STATE_EXPIRY_SECONDS,
        }
        if device_code is not None:
            payload["device_code"] = device_code
        payload_bytes = json.dumps(payload, separators=(",", ":")).encode()
        payload_b64 = urlsafe_b64encode(payload_bytes).rstrip(b"=").decode()

        signature = hmac.new(self._config.secret.encode(), payload_bytes, hashlib.sha256).digest()
        signature_b64 = urlsafe_b64encode(signature).rstrip(b"=").decode()

        return f"{payload_b64}.{signature_b64}"

    def verify_oauth_state(self, state: str) -> OAuthStateData | None:
        """Verify a signed state token and return structured state data if valid.

        Args:
            state: The signed state token to verify

        Returns:
            OAuthStateData if valid, None if invalid or expired
        """
        try:
            parts = state.split(".")
            if len(parts) != 2:
                return None

            payload_b64, signature_b64 = parts

            # Restore base64 padding
            payload_bytes = urlsafe_b64decode(payload_b64 + "==")
            signature = urlsafe_b64decode(signature_b64 + "==")

            # Verify signature
            expected_sig = hmac.new(
                self._config.secret.encode(), payload_bytes, hashlib.sha256
            ).digest()
            if not hmac.compare_digest(signature, expected_sig):
                logger.warning("OAuth state signature verification failed")
                return None

            # Parse and check expiry
            payload = json.loads(payload_bytes)
            if payload.get("exp", 0) < time.time():
                logger.warning("OAuth state expired")
                return None

            redirect_uri = payload.get("redirect_uri")
            provider = payload.get("provider")
            if not redirect_uri or not provider:
                logger.warning("OAuth state missing redirect_uri or provider")
                return None

            return OAuthStateData(
                redirect_uri=redirect_uri,
                provider=provider,
                device_code=payload.get("device_code"),
            )

        except Exception as e:
            logger.warning("OAuth state verification error: %s", e)
            return None
