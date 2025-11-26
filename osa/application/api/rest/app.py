from dishka import AsyncContainer, make_async_container
from dishka.integrations.fastapi import FastapiProvider, setup_dishka

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logfire

from osa.config import Config


def create_app() -> FastAPI:
    """Create FastAPI application."""
    config = Config()

    app_instance = FastAPI(
        title=config.server.name,
        description=config.server.description,
        version=config.server.version,
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
    setup_dishka(app_instance, container)

    # Register routes
    for module in modules:
        app_instance.include_router(module.api.rest.router)

    # TODO: Add error handlers

    return app_instance


# Create app instance for uvicorn
# Note: Logfire must be configured before this module is imported
# In production: start_app.py handles this
# In tests: configure in conftest.py
app = create_app()
