"""Dependency injection container."""

from dishka import AsyncContainer, make_async_container
from dishka.integrations.fastapi import FastapiProvider, setup_dishka

from osa.util.di.base import PROVIDERS, get_provider


def create_container() -> AsyncContainer:
    """Build production container (all prod implementations).

    Settings are loaded from environment variables automatically.

    Returns:
        Configured DI container with production providers
    """
    # Get provider instances - all are instantiated without arguments
    provider_instances = [get_provider(base, use_mock=False)() for base in PROVIDERS]
    # Include FastapiProvider for proper integration with FastAPI
    return make_async_container(*provider_instances, FastapiProvider())


def setup_di(app, container: AsyncContainer) -> None:
    """Setup dependency injection for FastAPI.

    Args:
        app: FastAPI application
        container: DI container
    """
    setup_dishka(container, app)
