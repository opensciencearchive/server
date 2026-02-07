"""Resource-level authorization checks for repo decorators."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from osa.domain.auth.model.role import Role


class ResourceCheck(ABC):
    """Base class for resource-level authorization checks.

    System identities bypass all checks. Anonymous identities are rejected.
    Principal identities are checked via the abstract _check method.
    """

    def evaluate(self, identity: Any, resource: Any) -> None:
        """Evaluate the check against the given identity and resource.

        Raises AuthorizationError if access is denied.
        """
        from osa.domain.auth.model.identity import System
        from osa.domain.auth.model.principal import Principal
        from osa.domain.shared.error import AuthorizationError

        if isinstance(identity, System):
            return  # Workers bypass all resource checks

        if not isinstance(identity, Principal):
            raise AuthorizationError("Authentication required", code="missing_token")

        self._check(identity, resource)

    @abstractmethod
    def _check(self, principal: Any, resource: Any) -> None:
        """Check authorization for an authenticated principal.

        Args:
            principal: The authenticated Principal
            resource: The domain resource being accessed

        Raises:
            AuthorizationError: If principal is not authorized for this resource.
        """
        ...

    def __or__(self, other: ResourceCheck) -> AnyOf:
        return AnyOf(checks=(self, other))


@dataclass(frozen=True)
class OwnerCheck(ResourceCheck):
    """Check that the principal owns the resource (resource.owner_id == principal.user_id)."""

    def _check(self, principal: Any, resource: Any) -> None:
        from osa.domain.shared.error import AuthorizationError

        owner_id = getattr(resource, "owner_id", None)
        if owner_id is None or owner_id != principal.user_id:
            raise AuthorizationError("Access denied: not resource owner", code="access_denied")


@dataclass(frozen=True)
class HasRole(ResourceCheck):
    """Check that the principal has at least the given role."""

    role: "Role"

    def _check(self, principal: Any, resource: Any) -> None:
        from osa.domain.shared.error import AuthorizationError

        if not principal.has_role(self.role):
            raise AuthorizationError(
                f"Access denied: requires role {self.role.name}",
                code="access_denied",
            )


@dataclass(frozen=True)
class AnyOf(ResourceCheck):
    """Check that at least one of the sub-checks passes."""

    checks: tuple[ResourceCheck, ...]

    def _check(self, principal: Any, resource: Any) -> None:
        from osa.domain.shared.error import AuthorizationError

        for check in self.checks:
            try:
                check._check(principal, resource)
                return  # At least one passed
            except AuthorizationError:
                continue

        raise AuthorizationError("Access denied", code="access_denied")

    def __or__(self, other: ResourceCheck) -> AnyOf:
        return AnyOf(checks=(*self.checks, other))


def owner() -> OwnerCheck:
    """Check that the principal owns the resource."""
    return OwnerCheck()


def has_role(role: "Role") -> HasRole:
    """Check that the principal has at least the given role."""
    return HasRole(role=role)
