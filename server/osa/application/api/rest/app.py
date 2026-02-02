import logging
from contextlib import asynccontextmanager

import logfire
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from osa.application.api.v1.errors import map_osa_error
from osa.application.api.v1.routes import events, health, records, search, stats, validation
from osa.application.di import create_container
from osa.config import Config, configure_logging
from osa.domain.shared.error import OSAError
from osa.infrastructure.event.worker import WorkerPool
from osa.infrastructure.source.discovery import validate_sources_at_startup
from osa.util.di.fastapi import setup_dishka

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    container = app.state.dishka_container

    # Run unified worker pool (pull-based event handlers + scheduled tasks)
    worker_pool = await container.get(WorkerPool)

    async with worker_pool:
        yield

    await container.close()


def create_app() -> FastAPI:
    """Create FastAPI application."""
    config = Config()

    # Configure logging early
    configure_logging(config.logging)
    logger.info("Starting OSA server: %s v%s", config.server.name, config.server.version)

    # Validate source configs at startup (fail fast with clear errors)
    validate_sources_at_startup(config.sources)

    app_instance = FastAPI(
        title=config.server.name,
        description=config.server.description,
        version=config.server.version,
        lifespan=lifespan,
    )

    # Instrument FastAPI for automatic tracing of HTTP requests
    logfire.instrument_httpx()
    logfire.instrument_fastapi(app_instance)

    # Setup dependency injection
    container = create_container()
    setup_dishka(container, app_instance)

    # Register v1 routes with /api/v1 prefix
    app_instance.include_router(health.router, prefix="/api/v1")
    app_instance.include_router(events.router, prefix="/api/v1")
    app_instance.include_router(records.router, prefix="/api/v1")
    app_instance.include_router(search.router, prefix="/api/v1")
    app_instance.include_router(stats.router, prefix="/api/v1")
    app_instance.include_router(validation.router, prefix="/api/v1")

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


# Create app instance for uvicorn
# Note: Logfire must be configured before this module is imported
# In production: start_app.py handles this
# In tests: configure in conftest.py
app = create_app()
