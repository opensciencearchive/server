"""RefreshToken entity for the auth domain."""

from datetime import UTC, datetime, timedelta

from osa.domain.auth.model.value import RefreshTokenId, TokenFamilyId, UserId
from osa.domain.shared.model.entity import Entity


class RefreshToken(Entity):
    """An opaque refresh token for session management.

    Tokens belong to a "family" for theft detection. When a token is refreshed,
    the new token inherits the family_id. If a revoked token is reused, the
    entire family is revoked (indicating potential theft).

    Invariants:
    - `token_hash` is a SHA256 hash (64 hex characters)
    - `expires_at` is always in the future at creation time
    - Once `revoked_at` is set, it cannot be unset
    """

    id: RefreshTokenId
    user_id: UserId
    token_hash: str  # SHA256 hash of the actual token value
    family_id: TokenFamilyId
    expires_at: datetime
    created_at: datetime
    revoked_at: datetime | None = None

    @property
    def is_valid(self) -> bool:
        """Token is valid if not revoked and not expired."""
        return self.revoked_at is None and self.expires_at > datetime.now(UTC)

    @property
    def is_revoked(self) -> bool:
        """Check if the token has been revoked."""
        return self.revoked_at is not None

    @property
    def is_expired(self) -> bool:
        """Check if the token has expired."""
        return self.expires_at <= datetime.now(UTC)

    def revoke(self) -> None:
        """Mark this token as revoked."""
        if self.revoked_at is None:
            self.revoked_at = datetime.now(UTC)

    @classmethod
    def create(
        cls,
        user_id: UserId,
        token_hash: str,
        family_id: TokenFamilyId,
        expires_in_days: int = 7,
    ) -> "RefreshToken":
        """Create a new refresh token."""
        now = datetime.now(UTC)
        return cls(
            id=RefreshTokenId.generate(),
            user_id=user_id,
            token_hash=token_hash,
            family_id=family_id,
            expires_at=now + timedelta(days=expires_in_days),
            created_at=now,
            revoked_at=None,
        )
