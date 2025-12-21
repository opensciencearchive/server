import logging
from contextlib import asynccontextmanager

import logfire
from osa.util.di.fastapi import setup_dishka
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from osa.application.api.rest.routes import health
from osa.application.di import create_container
from osa.config import Config, configure_logging
from osa.domain.search.api.rest import router as search_router
from osa.infrastructure.event.worker import BackgroundWorker

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    container = app.state.dishka_container

    # Run background worker (emits ServerStarted internally)
    worker = await container.get(BackgroundWorker)
    async with worker:
        yield

    await container.close()


def create_app() -> FastAPI:
    """Create FastAPI application."""
    config = Config()

    # Configure logging early
    configure_logging(config.logging)
    logger.info("Starting OSA server: %s v%s", config.server.name, config.server.version)

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

    # Register routes
    app_instance.include_router(health.router)
    app_instance.include_router(search_router)

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
