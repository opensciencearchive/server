"""Custom Dishka FastAPI integration using Scope.UOW."""

import logging
from typing import Any
from uuid import UUID

import jwt
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from starlette.requests import Request
from starlette.types import ASGIApp, Receive, Scope, Send
from starlette.websockets import WebSocket

from dishka import AsyncContainer

from osa.domain.auth.model.identity import Anonymous, Identity
from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.auth.model.value import ProviderIdentity, UserId
from osa.domain.auth.service.token import TokenService
from osa.infrastructure.persistence.tables import role_assignments_table
from osa.util.di.scope import Scope as OSAScope

logger = logging.getLogger(__name__)


async def resolve_identity(
    request: Request,
    token_service: TokenService,
    session_factory: async_sessionmaker[AsyncSession],
) -> Identity:
    """Resolve Identity from an HTTP request.

    Parses the JWT from the Authorization header and looks up roles.
    Returns Anonymous for unauthenticated or invalid requests.
    Uses a short-lived read-only session for the role lookup.
    """
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return Anonymous()

    token = auth_header[7:]  # Remove "Bearer " prefix

    try:
        payload: dict[str, Any] = token_service.validate_access_token(token)
    except (jwt.ExpiredSignatureError, jwt.InvalidTokenError):
        return Anonymous()

    user_id = UserId(UUID(payload["sub"]))

    # Lightweight role lookup using a short-lived session
    async with session_factory() as session:
        stmt = select(role_assignments_table.c.role).where(
            role_assignments_table.c.user_id == str(user_id)
        )
        result = await session.execute(stmt)
        roles = frozenset(Role[row.upper()] for (row,) in result)

    logger.debug(
        "Identity resolved: user_id=%s, roles=%s",
        user_id,
        roles,
    )

    return Principal(
        user_id=user_id,
        provider_identity=ProviderIdentity(
            provider=payload["provider"],
            external_id=payload["external_id"],
        ),
        roles=roles,
    )


class ContainerMiddleware:
    """ASGI middleware that creates a Scope.UOW container for each request.

    This is a custom version of dishka.integrations.starlette.ContainerMiddleware
    that uses our custom Scope.UOW instead of dishka.Scope.REQUEST.

    Resolves Identity from the request before entering the UOW scope so that
    both HTTP requests and background workers can provide Identity via context.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
    ) -> None:
        if scope["type"] not in ("http", "websocket"):
            return await self.app(scope, receive, send)

        request: Request | WebSocket
        context: dict[type, Any]

        if scope["type"] == "http":
            request = Request(scope, receive=receive, send=send)
            container: AsyncContainer = request.app.state.dishka_container

            # Resolve Identity before entering UOW scope
            token_service = await container.get(TokenService)
            session_factory = await container.get(async_sessionmaker[AsyncSession])
            identity = await resolve_identity(request, token_service, session_factory)

            context = {Request: request, Identity: identity}
        else:
            request = WebSocket(scope, receive, send)
            context = {WebSocket: request, Identity: Anonymous()}

        # Use Scope.UOW for both HTTP and WebSocket requests
        async with request.app.state.dishka_container(
            context,
            scope=OSAScope.UOW,
        ) as request_container:
            request.state.dishka_container = request_container
            return await self.app(scope, receive, send)


def setup_dishka(container: AsyncContainer, app) -> None:
    """Setup Dishka DI with custom Scope.UOW middleware.

    Args:
        container: The async DI container
        app: FastAPI or Starlette application
    """
    app.add_middleware(ContainerMiddleware)
    app.state.dishka_container = container
