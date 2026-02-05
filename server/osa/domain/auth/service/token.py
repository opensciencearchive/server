"""Token service for JWT creation and validation."""

import hashlib
import secrets
from datetime import UTC, datetime, timedelta
from typing import Any

import jwt

from osa.config import JwtConfig
from osa.domain.auth.model.value import UserId
from osa.domain.shared.service import Service


class TokenService(Service):
    """Service for JWT access token and refresh token operations.

    - Access tokens are JWTs (HS256) with user claims
    - Refresh tokens are opaque random strings, stored as hashes in the database
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
