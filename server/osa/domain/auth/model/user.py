"""User aggregate for the auth domain."""

from datetime import UTC, datetime

from osa.domain.auth.model.value import UserId
from osa.domain.shared.model.aggregate import Aggregate


class User(Aggregate):
    """An authenticated user in the OSA system.

    Users are created on first authentication via any identity provider.
    A user may have multiple linked identities (e.g., ORCiD + institutional SAML).

    Invariants:
    - `id` is immutable after creation
    - `created_at` is immutable after creation
    - `updated_at` is set on any modification
    """

    id: UserId
    display_name: str | None
    created_at: datetime
    updated_at: datetime | None = None

    @classmethod
    def create(cls, display_name: str | None = None) -> "User":
        """Create a new user."""
        now = datetime.now(UTC)
        return cls(
            id=UserId.generate(),
            display_name=display_name,
            created_at=now,
            updated_at=None,
        )

    def update_display_name(self, display_name: str | None) -> None:
        """Update the user's display name."""
        self.display_name = display_name
        self.updated_at = datetime.now(UTC)
