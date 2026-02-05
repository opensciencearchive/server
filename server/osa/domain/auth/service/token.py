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

from osa.config import JwtConfig
from osa.domain.auth.model.value import UserId
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

    def create_access_token(
        self,
        user_id: UserId,
        orcid_id: str,
        additional_claims: dict[str, Any] | None = None,
    ) -> str:
        """Create a JWT access token.

        Args:
            user_id: The user's internal ID
            orcid_id: The user's ORCiD ID
            additional_claims: Optional extra claims to include

        Returns:
            Encoded JWT string
        """
        now = datetime.now(UTC)
        expires_at = now + timedelta(minutes=self._config.access_token_expire_minutes)

        payload = {
            "sub": str(user_id),
            "orcid_id": orcid_id,
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

        Args:
            token: The JWT string to validate

        Returns:
            Decoded payload dict

        Raises:
            jwt.InvalidTokenError: If token is invalid or expired
        """
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

    def create_oauth_state(self, redirect_uri: str) -> str:
        """Create a signed, self-verifying OAuth state token.

        The state contains: nonce, redirect_uri, expiry timestamp.
        Signed with HMAC-SHA256 using the JWT secret.

        Args:
            redirect_uri: The URI to redirect to after OAuth completes

        Returns:
            URL-safe signed state token in format: payload.signature
        """
        payload = {
            "nonce": secrets.token_urlsafe(16),
            "redirect_uri": redirect_uri,
            "exp": int(time.time()) + STATE_EXPIRY_SECONDS,
        }
        payload_bytes = json.dumps(payload, separators=(",", ":")).encode()
        payload_b64 = urlsafe_b64encode(payload_bytes).rstrip(b"=").decode()

        signature = hmac.new(self._config.secret.encode(), payload_bytes, hashlib.sha256).digest()
        signature_b64 = urlsafe_b64encode(signature).rstrip(b"=").decode()

        return f"{payload_b64}.{signature_b64}"

    def verify_oauth_state(self, state: str) -> str | None:
        """Verify a signed state token and return the redirect_uri if valid.

        Args:
            state: The signed state token to verify

        Returns:
            The redirect_uri if valid, None if invalid or expired
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

            return payload.get("redirect_uri")

        except Exception as e:
            logger.warning("OAuth state verification error: %s", e)
            return None
