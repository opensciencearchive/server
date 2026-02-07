"""PostgreSQL implementation of RoleAssignmentRepository."""

from uuid import UUID

from sqlalchemy import delete, insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.auth.model.role import Role
from osa.domain.auth.model.role_assignment import RoleAssignment, RoleAssignmentId
from osa.domain.auth.model.value import UserId
from osa.domain.auth.port.role_repository import RoleAssignmentRepository
from osa.infrastructure.persistence.tables import role_assignments_table


def _row_to_role_assignment(row: dict) -> RoleAssignment:
    """Convert a database row to a RoleAssignment model."""
    return RoleAssignment(
        id=RoleAssignmentId(UUID(row["id"])),
        user_id=UserId(UUID(row["user_id"])),
        role=Role[row["role"].upper()],
        assigned_by=UserId(UUID(row["assigned_by"])),
        assigned_at=row["assigned_at"],
    )


def _role_assignment_to_dict(assignment: RoleAssignment) -> dict:
    """Convert a RoleAssignment model to a database row dict."""
    return {
        "id": str(assignment.id),
        "user_id": str(assignment.user_id),
        "role": assignment.role.name.lower(),
        "assigned_by": str(assignment.assigned_by),
        "assigned_at": assignment.assigned_at,
    }


class PostgresRoleAssignmentRepository(RoleAssignmentRepository):
    """PostgreSQL implementation of RoleAssignmentRepository."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_by_user_id(self, user_id: UserId) -> list[RoleAssignment]:
        stmt = select(role_assignments_table).where(
            role_assignments_table.c.user_id == str(user_id)
        )
        result = await self.session.execute(stmt)
        rows = result.mappings().all()
        return [_row_to_role_assignment(dict(row)) for row in rows]

    async def save(self, assignment: RoleAssignment) -> None:
        assignment_dict = _role_assignment_to_dict(assignment)
        stmt = insert(role_assignments_table).values(**assignment_dict)
        await self.session.execute(stmt)
        await self.session.flush()

    async def delete(self, user_id: UserId, role: Role) -> bool:
        stmt = delete(role_assignments_table).where(
            role_assignments_table.c.user_id == str(user_id),
            role_assignments_table.c.role == role.name.lower(),
        )
        result = await self.session.execute(stmt)
        await self.session.flush()
        return result.rowcount > 0

    async def get(self, user_id: UserId, role: Role) -> RoleAssignment | None:
        stmt = select(role_assignments_table).where(
            role_assignments_table.c.user_id == str(user_id),
            role_assignments_table.c.role == role.name.lower(),
        )
        result = await self.session.execute(stmt)
        row = result.mappings().first()
        return _row_to_role_assignment(dict(row)) if row else None
