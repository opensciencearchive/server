"""AssignRole command and handler."""

from datetime import datetime
from uuid import UUID

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.auth.model.value import UserId
from osa.domain.auth.service.authorization import AuthorizationService
from osa.domain.shared.authorization.gate import at_least
from osa.domain.shared.command import Command, CommandHandler, Result


class AssignRole(Command):
    """Command to assign a role to a user."""

    user_id: str  # UUID as string from API
    role: str  # Role name from API


class AssignRoleResult(Result):
    """Result containing the created role assignment."""

    id: str
    user_id: str
    role: str
    assigned_by: str
    assigned_at: datetime


class AssignRoleHandler(CommandHandler[AssignRole, AssignRoleResult]):
    __auth__ = at_least(Role.SUPERADMIN)
    principal: Principal
    authorization_service: AuthorizationService

    async def run(self, cmd: AssignRole) -> AssignRoleResult:
        assignment = await self.authorization_service.assign_role(
            user_id=UserId(UUID(cmd.user_id)),
            role=Role[cmd.role.upper()],
            assigned_by=self.principal.user_id,
        )

        return AssignRoleResult(
            id=str(assignment.id),
            user_id=str(assignment.user_id),
            role=assignment.role.name.lower(),
            assigned_by=str(assignment.assigned_by),
            assigned_at=assignment.assigned_at,
        )
