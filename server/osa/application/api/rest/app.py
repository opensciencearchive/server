import logging
import os
import sys
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Any

import logfire
from dishka import Provider as DishkaProvider
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from slowapi.errors import RateLimitExceeded
from sqlalchemy.ext.asyncio import AsyncEngine
from starlette.routing import Route

from osa.application.api.v1.errors import map_osa_error
from osa.application.api.v1.routes import (
    admin,
    auth,
    conventions,
    depositions,
    events,
    hooks,
    ingesters,
    ingestions,
    health,
    metrics,
    ontologies,
    schemas,
    stats,
    validation,
)
from osa.application.api.v1.routes import data as data_routes
from osa.application.api.mcp.server import McpSurface
from osa.application.api.rest import skill as skill_routes
from osa.application.api.v1.routes.data._limiter import limiter
from osa.application.di import create_container
from osa.config import DEV_JWT_SECRET, Config
from osa.domain.shared.authorization.startup import validate_all_handlers
from osa.domain.shared.error import OSAError
from osa.domain.shared.event import EventHandler
from osa.infrastructure.event.worker import WorkerPool
from osa.infrastructure.persistence.seed import ensure_system_user
from osa.infrastructure.telemetry.api import ApiInstrumentation
from osa.infrastructure.telemetry.setup import bootstrap
from osa.util.di.fastapi import setup_dishka

logger = logging.getLogger(__name__)


def _check_dev_secret_safety(config: Config) -> None:
    """Refuse to start when the well-known dev JWT secret is misconfigured.

    The dev JWT secret is public knowledge — anyone with it can mint admin
    tokens. It's safe to ship only when both:
      1. The operator explicitly opted into dev mode (OSA_DEV_MODE=true).
      2. The server is bound to loopback so the secret isn't network-reachable.

    The second check is best-effort: we read the bind address from common
    uvicorn / OSA env vars and fall back to allowing the boot if no signal is
    present (dev mode alone is the operator's stated intent).
    """
    if config.auth.jwt.secret != DEV_JWT_SECRET:
        return

    if not config.dev_mode:
        raise RuntimeError(
            "Refusing to start: JWT secret is the well-known local-dev default "
            "but OSA_DEV_MODE is not set. Configure OSA_AUTH__JWT__SECRET with a "
            "real secret (openssl rand -hex 32), or set OSA_DEV_MODE=true for "
            "local development."
        )

    # Best-effort: refuse to bind the dev secret to a public interface.
    bind_host = os.environ.get("UVICORN_HOST") or os.environ.get("OSA_HOST") or ""
    loopback_hosts = {"", "127.0.0.1", "localhost", "::1"}
    if bind_host and bind_host not in loopback_hosts:
        raise RuntimeError(
            f"Refusing to start: JWT secret is the well-known local-dev default "
            f"and bind host is {bind_host!r} (not loopback). Either set "
            "OSA_AUTH__JWT__SECRET to a real secret, or bind to 127.0.0.1."
        )

    # logfire isn't configured yet at this point (configure() runs after this
    # check in create_app), and logfire silently drops pre-configure calls.
    # Write to stderr directly so the warning is always visible.
    print(
        "WARNING: Running on the well-known local-dev JWT secret. "
        "This is safe for local development only — never use in production.",
        file=sys.stderr,
        flush=True,
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    container = app.state.dishka_container

    # Seed required system rows before anything else
    engine = await container.get(AsyncEngine)
    await ensure_system_user(engine)

    # Run unified worker pool (pull-based event handlers + scheduled tasks)
    worker_pool = await container.get(WorkerPool)

    async with AsyncExitStack() as stack:
        await stack.enter_async_context(worker_pool)
        # MCP Apps surface (#162): renders SKILL.md instructions from the live
        # catalog and runs the streamable-HTTP session manager.
        mcp_surface: McpSurface | None = getattr(app.state, "mcp_surface", None)
        if mcp_surface is not None:
            await stack.enter_async_context(mcp_surface.lifespan())
        yield

    # Flush buffered telemetry (span/metric/log exporters) before teardown.
    # NOT logfire.shutdown(): that tears down the process-global providers,
    # which would break later create_app() instances in the same test process
    # (the bootstrap is configure-once and never re-runs).
    logfire.force_flush()

    await container.close()


def create_app(
    *,
    providers: list[DishkaProvider] | None = None,
    extra_handlers: list[type[EventHandler[Any]]] | None = None,
) -> FastAPI:
    """Create FastAPI application.

    This is the main entry point for running OSA. External hosts (e.g.
    Amacrin) use the keyword arguments to customise the runtime without
    duplicating any app wiring::

        app = create_app(
            providers=[K8sProvider()],
            extra_handlers=[MeterUsage, SendNotification],
        )

    Args:
        providers: Extra Dishka providers that override the built-in
            bindings. For example, pass a ``K8sProvider`` to replace the
            default OCI container runner with a Kubernetes-based one.
        extra_handlers: Extra event handler types registered alongside
            the core handlers for subscription routing, WorkerPool
            registration, and DI resolution.
    """
    # Pydantic Settings populates from env vars at runtime
    config = Config()

    # Refuse to boot if the dev JWT secret is misconfigured for the deploy.
    _check_dev_secret_safety(config)

    # Configure the process-global telemetry pipeline (logfire + OTel exporters).
    # Idempotent across repeated create_app() calls in one process.
    bootstrap.configure(config)

    logfire.info("Starting OSA server: {name} v{version}", name=config.name, version=config.version)

    app_instance = FastAPI(
        title=config.name,
        description=config.description,
        version=config.version,
        lifespan=lifespan,
    )

    logfire.instrument_httpx()
    logfire.instrument_fastapi(
        app_instance,
        # Probes and the scrape endpoint are high-frequency and uninteresting as
        # traces; the OTel FastAPI instrumentor takes a comma-separated regex list.
        excluded_urls="/api/v1/health,/api/v1/ready,/metrics",
    )

    # Setup dependency injection
    container = create_container(
        *(providers or []),
        extra_handlers=extra_handlers,
    )

    # Validate all handlers have authorization declarations (fail fast). Walks
    # the container's own registry, so this needs the container built first.
    validate_all_handlers(container)

    setup_dishka(container, app_instance)

    # Register v1 routes with /api/v1 prefix
    app_instance.include_router(health.router, prefix="/api/v1")
    app_instance.include_router(admin.router, prefix="/api/v1")
    app_instance.include_router(auth.router, prefix="/api/v1")
    app_instance.include_router(events.router, prefix="/api/v1")
    app_instance.include_router(stats.router, prefix="/api/v1")
    app_instance.include_router(ontologies.router, prefix="/api/v1")
    app_instance.include_router(schemas.router, prefix="/api/v1")
    app_instance.include_router(conventions.router, prefix="/api/v1")
    app_instance.include_router(hooks.router, prefix="/api/v1")
    app_instance.include_router(ingesters.router, prefix="/api/v1")
    app_instance.include_router(depositions.router, prefix="/api/v1")
    app_instance.include_router(ingestions.router, prefix="/api/v1")
    app_instance.include_router(validation.router, prefix="/api/v1")
    app_instance.include_router(data_routes.router, prefix="/api/v1")

    # Unversioned root surface — GET / and GET /SKILL.md (#151), plus GET
    # /metrics (#158, Prometheus convention). Included after the /api/v1
    # routers so nothing shadows the API prefix.
    app_instance.include_router(skill_routes.router)
    app_instance.include_router(metrics.router)

    # MCP Apps surface (#162) — streamable-HTTP endpoint at /mcp, same
    # unversioned tier as the skill surface. An exact-path Route (the surface
    # object is a raw ASGI endpoint), NOT a Mount: Mount would 307-redirect
    # bare /mcp to /mcp/, which not every MCP client follows. The session
    # manager only accepts requests once the app lifespan has entered
    # `mcp_surface.lifespan()`.
    if config.mcp.enabled:
        mcp_surface = McpSurface(container, config)
        app_instance.state.mcp_surface = mcp_surface
        app_instance.router.routes.append(
            Route("/mcp", endpoint=mcp_surface, methods=["GET", "POST", "DELETE"])
        )

    # POST /data/* rate limiting (slowapi). The shared limiter is attached to
    # app state and its 429 handler registered; GET routes are not limited.
    app_instance.state.limiter = limiter

    @app_instance.exception_handler(RateLimitExceeded)
    async def rate_limit_handler(request: Request, exc: RateLimitExceeded):
        return JSONResponse(
            status_code=429,
            content={"code": "rate_limited", "message": "POST rate limit exceeded (10/min/IP)."},
        )

    # Global OSA error handler - maps domain and infrastructure errors to HTTP responses
    @app_instance.exception_handler(OSAError)
    async def osa_error_handler(request: Request, exc: OSAError):
        http_exc = map_osa_error(exc)
        return JSONResponse(
            status_code=http_exc.status_code,
            content=http_exc.detail,
        )

    # Global exception handler - logs all unhandled exceptions
    @app_instance.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        logger.exception(
            "Unhandled exception on %s %s",
            request.method,
            request.url.path,
        )
        # Count the unhandled error for observability. A telemetry failure must
        # never mask the 500 response, so resolution + emission is guarded and
        # only logged.
        try:
            instrumentation = await request.app.state.dishka_container.get(ApiInstrumentation)
            instrumentation.unhandled_error()
        except Exception:
            logger.warning("Failed to record unhandled-error metric", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error"},
        )

    return app_instance
