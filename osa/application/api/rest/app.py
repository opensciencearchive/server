from contextlib import asynccontextmanager
from dishka.integrations.fastapi import setup_dishka

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logfire

from osa.application.api.rest.routes import health, search
from osa.application.di import create_container
from osa.config import Config


@asynccontextmanager
async def lifespan(app: FastAPI):
    # TODO: Wire up event listeners when outbox drainer is implemented
    yield
    await app.state.dishka_container.close()


def create_app() -> FastAPI:
    """Create FastAPI application."""
    config = Config()

    app_instance = FastAPI(
        title=config.server.name,
        description=config.server.description,
        version=config.server.version,
        lifespan=lifespan,
    )

    # Instrument FastAPI for automatic tracing of HTTP requests
    logfire.instrument_httpx()
    logfire.instrument_fastapi(app_instance)

    app_instance.add_middleware(
        CORSMiddleware,
        allow_origins=[
            config.frontend.url,
        ],
        allow_credentials=True,  # TODO: need?
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
        allow_headers=[
            "Authorization",
            "Content-Type",
            "Accept",
            "Origin",
            "User-Agent",
            "DNT",
            "Cache-Control",
            "X-Requested-With",
        ],
        expose_headers=["Content-Length", "Content-Type"],
        max_age=600,  # Cache preflight requests for 10 minutes
    )

    # Setup dependency injection
    container = create_container()
    setup_dishka(container, app_instance)

    # Register routes
    app_instance.include_router(health.router)
    app_instance.include_router(search.router)

    # TODO: Add error handlers

    return app_instance


# Create app instance for uvicorn
# Note: Logfire must be configured before this module is imported
# In production: start_app.py handles this
# In tests: configure in conftest.py
app = create_app()
