"""Authorization service — role assignment management."""

from osa.domain.auth.model.role import Role
from osa.domain.auth.model.role_assignment import RoleAssignment
from osa.domain.auth.model.value import UserId
from osa.domain.auth.port.role_repository import RoleAssignmentRepository
from osa.domain.shared.error import ConflictError, NotFoundError
from osa.domain.shared.service import Service


class AuthorizationService(Service):
    """Manages role assignments for users."""

    _role_repo: RoleAssignmentRepository

    async def assign_role(
        self,
        user_id: UserId,
        role: Role,
        assigned_by: UserId,
    ) -> RoleAssignment:
        """Assign a role to a user. Raises ConflictError if already assigned."""
        existing = await self._role_repo.get(user_id, role)
        if existing is not None:
            raise ConflictError(
                f"Role {role.name} already assigned to user {user_id}",
                code="role_already_assigned",
            )

        assignment = RoleAssignment.create(
            user_id=user_id,
            role=role,
            assigned_by=assigned_by,
        )
        await self._role_repo.save(assignment)
        return assignment

    async def revoke_role(self, user_id: UserId, role: Role) -> None:
        """Revoke a role from a user. Raises NotFoundError if not assigned."""
        deleted = await self._role_repo.delete(user_id, role)
        if not deleted:
            raise NotFoundError(
                f"Role {role.name} not assigned to user {user_id}",
                code="role_not_found",
            )

    async def list_roles(self, user_id: UserId) -> list[RoleAssignment]:
        """List all role assignments for a user."""
        return await self._role_repo.get_by_user_id(user_id)
