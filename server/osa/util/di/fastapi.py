"""Custom Dishka FastAPI integration using Scope.UOW."""

from starlette.requests import Request
from starlette.types import ASGIApp, Receive, Scope, Send
from starlette.websockets import WebSocket

from dishka import AsyncContainer

from osa.util.di.scope import Scope as OSAScope


class ContainerMiddleware:
    """ASGI middleware that creates a Scope.UOW container for each request.

    This is a custom version of dishka.integrations.starlette.ContainerMiddleware
    that uses our custom Scope.UOW instead of dishka.Scope.REQUEST.
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
        context: dict[type[Request | WebSocket], Request | WebSocket]

        if scope["type"] == "http":
            request = Request(scope, receive=receive, send=send)
            context = {Request: request}
        else:
            request = WebSocket(scope, receive, send)
            context = {WebSocket: request}

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
