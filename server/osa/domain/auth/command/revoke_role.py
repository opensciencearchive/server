"""RevokeRole command and handler."""

from uuid import UUID

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.auth.model.value import UserId
from osa.domain.auth.service.authorization import AuthorizationService
from osa.domain.shared.authorization.policy import requires_role
from osa.domain.shared.command import Command, CommandHandler, Result


class RevokeRole(Command):
    """Command to revoke a role from a user."""

    user_id: str  # UUID as string from API
    role: str  # Role name from API


class RevokeRoleResult(Result):
    """Empty result for successful revocation."""

    pass


class RevokeRoleHandler(CommandHandler[RevokeRole, RevokeRoleResult]):
    __auth__ = requires_role(Role.SUPERADMIN)
    _principal: Principal | None = None
    authorization_service: AuthorizationService

    async def run(self, cmd: RevokeRole) -> RevokeRoleResult:
        await self.authorization_service.revoke_role(
            user_id=UserId(UUID(cmd.user_id)),
            role=Role[cmd.role.upper()],
        )
        return RevokeRoleResult()
