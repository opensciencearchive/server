"""GetUserRoles query and handler."""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.auth.model.value import UserId
from osa.domain.auth.service.authorization import AuthorizationService
from osa.domain.shared.authorization.policy import requires_role
from osa.domain.shared.query import Query, QueryHandler
from osa.domain.shared.query import Result as QueryResult


class GetUserRoles(Query):
    """Query to get all roles assigned to a user."""

    user_id: str  # UUID as string from API


class RoleAssignmentDTO(BaseModel):
    id: str
    user_id: str
    role: str
    assigned_by: str
    assigned_at: datetime


class GetUserRolesResult(QueryResult):
    roles: list[RoleAssignmentDTO]


class GetUserRolesHandler(QueryHandler[GetUserRoles, GetUserRolesResult]):
    __auth__ = requires_role(Role.SUPERADMIN)
    _principal: Principal | None = None
    authorization_service: AuthorizationService

    async def run(self, cmd: GetUserRoles) -> GetUserRolesResult:
        assignments = await self.authorization_service.list_roles(
            user_id=UserId(UUID(cmd.user_id)),
        )

        return GetUserRolesResult(
            roles=[
                RoleAssignmentDTO(
                    id=str(a.id),
                    user_id=str(a.user_id),
                    role=a.role.name.lower(),
                    assigned_by=str(a.assigned_by),
                    assigned_at=a.assigned_at,
                )
                for a in assignments
            ]
        )
