"""Custom Dishka FastAPI integration using Scope.UOW."""

import logging
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

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


def _parse_scopes(payload: dict[str, Any], scope_claim: str) -> frozenset[str]:
    """Parse OAuth scopes from an M2M token (#145, US5).

    Tolerant of the two common encodings: a single space-delimited string
    (``"scope": "conventions:write hooks:write"``) or an array
    (``"scp": ["conventions:write"]``).
    """
    raw = payload.get(scope_claim)
    if isinstance(raw, str):
        return frozenset(raw.split())
    if isinstance(raw, (list, tuple)):
        return frozenset(str(s) for s in raw)
    return frozenset()


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
    if not auth_header:
        # Anonymous traffic is normal (health checks, public endpoints) — debug only.
        logger.debug("auth: no Authorization header")
        return Anonymous()
    if not auth_header.startswith("Bearer "):
        logger.warning(
            "auth: Authorization header is not Bearer (scheme=%r)",
            auth_header.split(" ", 1)[0],
        )
        return Anonymous()

    token = auth_header[7:]  # Remove "Bearer " prefix

    try:
        payload: dict[str, Any] = token_service.validate_access_token(token)
    except jwt.ExpiredSignatureError:
        logger.warning("auth: rejected expired token")
        return Anonymous()
    except jwt.InvalidTokenError as e:
        # Surface the JWT-lib reason class (e.g. InvalidSignatureError,
        # InvalidAudienceError) so a misaligned secret / aud is diagnosable.
        # We log only the exception class, never its message or the token.
        logger.warning("auth: rejected invalid token (%s)", type(e).__name__)
        return Anonymous()

    # M2M path (#145, US5): a token from the configured second issuer is
    # authorized by scopes, not DB roles. Its `sub` is a client identifier, not
    # an internal user UUID, so derive a stable synthetic UserId from issuer+sub
    # for provenance (`built_by`) and keep roles empty.
    extra_issuer = token_service.extra_issuer
    if extra_issuer is not None and payload.get("iss") == extra_issuer.issuer:
        client = str(payload.get("sub", ""))
        scopes = _parse_scopes(payload, extra_issuer.scope_claim)
        logger.debug("auth: M2M credential (client=%s, scopes=%s)", client, sorted(scopes))
        return Principal(
            user_id=UserId(uuid5(NAMESPACE_URL, f"{extra_issuer.issuer}#{client}")),
            provider_identity=ProviderIdentity(provider="m2m", external_id=client),
            roles=frozenset(),
            scopes=scopes,
        )

    user_id = UserId(UUID(payload["sub"]))

    # Lightweight role lookup using a short-lived session
    async with session_factory() as session:
        stmt = select(role_assignments_table.c.role).where(
            role_assignments_table.c.user_id == str(user_id)
        )
        result = await session.execute(stmt)
        roles = frozenset(Role[row.upper()] for (row,) in result)

    # user_id is an internal UUID, roles is a small enum set — safe to log at debug.
    # Provider / external_id are PII for federated identities (e.g. ORCID) and are
    # intentionally omitted from log output.
    logger.debug(
        "auth: authenticated (user_id=%s, roles=%s)",
        user_id,
        sorted(r.name for r in roles),
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
