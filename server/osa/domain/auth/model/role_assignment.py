"""RoleAssignment entity — tracks user-role associations."""

from datetime import UTC, datetime
from uuid import UUID, uuid4

from pydantic import RootModel

from osa.domain.auth.model.role import Role
from osa.domain.auth.model.value import UserId
from osa.domain.shared.model.entity import Entity


class RoleAssignmentId(RootModel[UUID]):
    """Unique identifier for a RoleAssignment."""

    @classmethod
    def generate(cls) -> "RoleAssignmentId":
        return cls(uuid4())

    def __str__(self) -> str:
        return str(self.root)

    def __hash__(self) -> int:
        return hash(self.root)


class RoleAssignment(Entity):
    """Association between a user and a role, managed by superadmins."""

    id: RoleAssignmentId
    user_id: UserId
    role: Role
    assigned_by: UserId
    assigned_at: datetime

    @classmethod
    def create(
        cls,
        user_id: UserId,
        role: Role,
        assigned_by: UserId,
    ) -> "RoleAssignment":
        return cls(
            id=RoleAssignmentId.generate(),
            user_id=user_id,
            role=role,
            assigned_by=assigned_by,
            assigned_at=datetime.now(UTC),
        )
