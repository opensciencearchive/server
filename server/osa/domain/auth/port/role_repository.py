"""Repository port for RoleAssignment persistence."""

from abc import abstractmethod
from typing import Protocol

from osa.domain.auth.model.role import Role
from osa.domain.auth.model.role_assignment import RoleAssignment
from osa.domain.auth.model.value import UserId
from osa.domain.shared.port import Port


class RoleAssignmentRepository(Port, Protocol):
    """Repository for RoleAssignment entity persistence."""

    @abstractmethod
    async def get_by_user_id(self, user_id: UserId) -> list[RoleAssignment]:
        """Get all role assignments for a user."""
        ...

    @abstractmethod
    async def save(self, assignment: RoleAssignment) -> None:
        """Save a role assignment."""
        ...

    @abstractmethod
    async def delete(self, user_id: UserId, role: Role) -> bool:
        """Delete a role assignment. Returns True if deleted, False if not found."""
        ...

    @abstractmethod
    async def get(self, user_id: UserId, role: Role) -> RoleAssignment | None:
        """Get a specific role assignment."""
        ...
