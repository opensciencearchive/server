"""LinkedAccount entity for the auth domain.

Links a User to an external identity provider (e.g. ORCiD, SAML).
"""

from datetime import UTC, datetime
from typing import Any

from osa.domain.auth.model.value import IdentityId, UserId
from osa.domain.shared.model.entity import Entity


class LinkedAccount(Entity):
    """A link between a User and an external identity provider.

    Examples:
    - ORCiD: provider="orcid", external_id="0000-0001-2345-6789"
    - SAML: provider="saml:university.edu", external_id="jdoe@university.edu"

    Invariants:
    - `(provider, external_id)` is globally unique
    - `user_id` is immutable after creation
    - `provider` and `external_id` are immutable after creation
    """

    id: IdentityId
    user_id: UserId
    provider: str
    external_id: str
    metadata: dict[str, Any] | None = None  # Provider-specific data (name, email)
    created_at: datetime

    @classmethod
    def create(
        cls,
        user_id: UserId,
        provider: str,
        external_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> "LinkedAccount":
        """Create a new identity link."""
        return cls(
            id=IdentityId.generate(),
            user_id=user_id,
            provider=provider,
            external_id=external_id,
            metadata=metadata,
            created_at=datetime.now(UTC),
        )
