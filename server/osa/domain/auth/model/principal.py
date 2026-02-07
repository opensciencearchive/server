"""Principal — authenticated identity with roles, resolved per-request."""

from dataclasses import dataclass

from osa.domain.auth.model.role import Role
from osa.domain.auth.model.value import ProviderIdentity, UserId


@dataclass(frozen=True)
class Principal:
    """The authenticated identity of the current requester.

    Resolved per-request from JWT + role lookup. Immutable after creation.
    """

    user_id: UserId
    identity: ProviderIdentity
    roles: frozenset[Role]

    def has_role(self, role: Role) -> bool:
        """Check if any assigned role >= the given role (hierarchy comparison)."""
        return any(r >= role for r in self.roles)

    def has_any_role(self, *roles: Role) -> bool:
        """Check if any assigned role satisfies any of the given roles."""
        return any(self.has_role(r) for r in roles)
