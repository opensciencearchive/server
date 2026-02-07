"""Admin routes for role management."""

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter, Response
from pydantic import BaseModel

from osa.domain.auth.command.assign_role import (
    AssignRole,
    AssignRoleHandler,
)
from osa.domain.auth.command.revoke_role import (
    RevokeRole,
    RevokeRoleHandler,
)
from osa.domain.auth.query.get_user_roles import (
    GetUserRoles,
    GetUserRolesHandler,
)

router = APIRouter(prefix="/admin", tags=["Admin"], route_class=DishkaRoute)


class AssignRoleRequest(BaseModel):
    """Request body for assigning a role."""

    role: str


class RoleAssignmentResponse(BaseModel):
    """Response for a single role assignment."""

    id: str
    user_id: str
    role: str
    assigned_by: str
    assigned_at: str


class RoleAssignmentListResponse(BaseModel):
    """Response listing role assignments."""

    roles: list[RoleAssignmentResponse]


@router.get("/users/{user_id}/roles", response_model=RoleAssignmentListResponse)
async def list_user_roles(
    user_id: str,
    handler: FromDishka[GetUserRolesHandler],
) -> RoleAssignmentListResponse:
    """List all roles assigned to a user. Requires SuperAdmin role."""
    result = await handler.run(GetUserRoles(user_id=user_id))
    return RoleAssignmentListResponse(
        roles=[
            RoleAssignmentResponse(
                id=r.id,
                user_id=r.user_id,
                role=r.role,
                assigned_by=r.assigned_by,
                assigned_at=r.assigned_at.isoformat(),
            )
            for r in result.roles
        ]
    )


@router.post(
    "/users/{user_id}/roles",
    response_model=RoleAssignmentResponse,
    status_code=201,
)
async def assign_role(
    user_id: str,
    body: AssignRoleRequest,
    handler: FromDishka[AssignRoleHandler],
) -> RoleAssignmentResponse:
    """Assign a role to a user. Requires SuperAdmin role."""
    result = await handler.run(AssignRole(user_id=user_id, role=body.role))
    return RoleAssignmentResponse(
        id=result.id,
        user_id=result.user_id,
        role=result.role,
        assigned_by=result.assigned_by,
        assigned_at=result.assigned_at.isoformat(),
    )


@router.delete("/users/{user_id}/roles/{role}", status_code=204)
async def revoke_role(
    user_id: str,
    role: str,
    handler: FromDishka[RevokeRoleHandler],
) -> Response:
    """Revoke a role from a user. Requires SuperAdmin role."""
    await handler.run(RevokeRole(user_id=user_id, role=role))
    return Response(status_code=204)
