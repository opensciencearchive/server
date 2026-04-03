import logging
import sys
from contextlib import asynccontextmanager
from typing import Any

import logfire
from dishka import Provider as DishkaProvider
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncEngine

from osa.application.api.v1.errors import map_osa_error
from osa.application.api.v1.routes import (
    admin,
    auth,
    conventions,
    depositions,
    discovery,
    events,
    ingestions,
    health,
    ontologies,
    records,
    schemas,
    search,
    stats,
    validation,
)
from osa.application.di import create_container
from osa.config import Config
from osa.domain.shared.authorization.startup import validate_all_handlers
from osa.domain.shared.error import OSAError
from osa.domain.shared.event import EventHandler
from osa.infrastructure.event.worker import WorkerPool
from osa.infrastructure.persistence.seed import ensure_system_user
from osa.util.di.fastapi import setup_dishka

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    container = app.state.dishka_container

    # Seed required system rows before anything else
    engine = await container.get(AsyncEngine)
    await ensure_system_user(engine)

    # Run unified worker pool (pull-based event handlers + scheduled tasks)
    worker_pool = await container.get(WorkerPool)

    async with worker_pool:
        yield

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
    config = Config()  # type: ignore[call-arg]

    # Configure logfire as the single logging system
    import logging as _logging

    from opentelemetry.sdk.trace.export import SimpleSpanProcessor

    from osa.infrastructure.logging import OSAConsoleExporter

    logfire.configure(
        send_to_logfire="if-token-present",
        service_name=config.name,
        console=False,  # Disable default console — we use OSAConsoleExporter
        inspect_arguments=False,
        additional_span_processors=[
            SimpleSpanProcessor(
                OSAConsoleExporter(
                    output=sys.stderr,
                    include_timestamp=True,
                    min_log_level=config.logging.level,
                )
            ),
        ],
    )

    # Route Python logging through logfire so old-style logger.info() calls
    # appear in the same output stream
    root = _logging.getLogger()
    root.setLevel(config.logging.level.upper())
    for h in root.handlers[:]:
        root.removeHandler(h)
    root.addHandler(logfire.LogfireLoggingHandler())

    # Suppress duplicate access logs — logfire FastAPI instrumentation handles HTTP logging
    _logging.getLogger("uvicorn.access").setLevel(_logging.WARNING)

    logfire.info("Starting OSA server: {name} v{version}", name=config.name, version=config.version)

    # Validate all handlers have authorization declarations (fail fast)
    validate_all_handlers()

    app_instance = FastAPI(
        title=config.name,
        description=config.description,
        version=config.version,
        lifespan=lifespan,
    )

    logfire.instrument_httpx()
    logfire.instrument_fastapi(
        app_instance,
        excluded_urls="/api/v1/health",
    )

    # Setup dependency injection
    container = create_container(
        *(providers or []),
        extra_handlers=extra_handlers,
    )
    setup_dishka(container, app_instance)

    # Register v1 routes with /api/v1 prefix
    app_instance.include_router(health.router, prefix="/api/v1")
    app_instance.include_router(admin.router, prefix="/api/v1")
    app_instance.include_router(auth.router, prefix="/api/v1")
    app_instance.include_router(events.router, prefix="/api/v1")
    app_instance.include_router(records.router, prefix="/api/v1")
    app_instance.include_router(search.router, prefix="/api/v1")
    app_instance.include_router(stats.router, prefix="/api/v1")
    app_instance.include_router(ontologies.router, prefix="/api/v1")
    app_instance.include_router(schemas.router, prefix="/api/v1")
    app_instance.include_router(conventions.router, prefix="/api/v1")
    app_instance.include_router(depositions.router, prefix="/api/v1")
    app_instance.include_router(ingestions.router, prefix="/api/v1")
    app_instance.include_router(validation.router, prefix="/api/v1")
    app_instance.include_router(discovery.router, prefix="/api/v1")

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
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error"},
        )

    return app_instance
