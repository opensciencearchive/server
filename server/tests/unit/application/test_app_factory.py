"""Unit tests for create_app and create_container extension points.

Tests that create_app() and create_container() correctly wire extra
providers and event handlers, enabling infrastructure to be swapped
at startup.
"""

import asyncio
import os
from pathlib import Path
from typing import ClassVar
from unittest.mock import patch

import pytest
from dishka import provide
from fastapi.testclient import TestClient

from osa.application.api.rest.app import create_app
from osa.application.di import create_container
from osa.domain.shared.event import Event, EventHandler, EventId
from osa.domain.shared.model.hook import HookDefinition
from osa.domain.shared.model.source import IngesterDefinition
from osa.domain.shared.model.subscription_registry import SubscriptionRegistry
from osa.domain.shared.port.ingester_runner import IngesterInputs, IngesterOutput, IngesterRunner
from osa.domain.validation.model.hook_result import HookResult, HookStatus
from osa.domain.validation.port.hook_runner import HookInputs, HookRunner
from osa.infrastructure.event.di import HandlerTypes, _CORE_HANDLERS
from osa.infrastructure.oci.runner import OciHookRunner
from osa.infrastructure.oci.ingester_runner import OciIngesterRunner
from osa.util.di.base import Provider
from osa.util.di.scope import Scope

# Minimal env for Config()
os.environ.setdefault(
    "OSA_AUTH__JWT__SECRET",
    "test-secret-that-is-at-least-32-characters-long",
)
os.environ.setdefault("OSA_BASE_URL", "http://localhost:8000")


# ---------------------------------------------------------------------------
# Stub runners
# ---------------------------------------------------------------------------


class StubHookRunner:
    """Stub HookRunner for testing provider overrides."""

    async def run(self, hook: HookDefinition, inputs: HookInputs, work_dir: Path) -> HookResult:
        return HookResult(hook_name=hook.name, status=HookStatus.PASSED, duration_seconds=0.0)


class StubIngesterRunner:
    """Stub IngesterRunner for testing provider overrides."""

    async def run(
        self,
        source: IngesterDefinition,
        inputs: IngesterInputs,
        files_dir: Path,
        work_dir: Path,
    ) -> IngesterOutput:
        return IngesterOutput(records=[], session=None, files_dir=files_dir)


class StubRunnerProvider(Provider):
    """Provides stub runners, overriding the default OCI ones."""

    @provide(scope=Scope.UOW, override=True)
    def get_hook_runner(self) -> HookRunner:
        return StubHookRunner()

    @provide(scope=Scope.UOW, override=True)
    def get_ingester_runner(self) -> IngesterRunner:
        return StubIngesterRunner()


# ---------------------------------------------------------------------------
# Stub event handler
# ---------------------------------------------------------------------------


class CustomEvent(Event):
    """Test event for extra handler registration."""

    id: EventId
    data: str


class CustomHandler(EventHandler[CustomEvent]):
    """Test handler registered via extra_handlers."""

    __poll_interval__: ClassVar[float] = 0.01

    async def handle(self, event: CustomEvent) -> None:
        pass


# ---------------------------------------------------------------------------
# Tests: create_container — provider overrides
# ---------------------------------------------------------------------------


class TestProviderOverrides:
    """Test that extra providers override default bindings."""

    def test_runner_override(self):
        """Extra provider replaces default OCI runners."""
        container = create_container(StubRunnerProvider())

        async def resolve():
            async with container(scope=Scope.UOW) as uow:
                hook = await uow.get(HookRunner)
                ingester = await uow.get(IngesterRunner)
                return hook, ingester

        hook_runner, ingester_runner = asyncio.run(resolve())
        assert isinstance(hook_runner, StubHookRunner)
        assert isinstance(ingester_runner, StubIngesterRunner)

    def test_default_runners_without_override(self):
        """Without extra providers, default OCI runners are used."""
        container = create_container()

        async def resolve():
            async with container(scope=Scope.UOW) as uow:
                hook = await uow.get(HookRunner)
                ingester = await uow.get(IngesterRunner)
                return hook, ingester

        hook_runner, ingester_runner = asyncio.run(resolve())
        assert isinstance(hook_runner, OciHookRunner)
        assert isinstance(ingester_runner, OciIngesterRunner)


# ---------------------------------------------------------------------------
# Tests: create_container — extra event handlers
# ---------------------------------------------------------------------------


class TestExtraHandlers:
    """Test that extra_handlers are wired into the event system."""

    def test_handler_resolvable_from_di(self):
        """Extra handlers can be instantiated by the DI container."""
        container = create_container(extra_handlers=[CustomHandler])

        async def resolve():
            async with container(scope=Scope.UOW) as uow:
                return await uow.get(CustomHandler)

        handler = asyncio.run(resolve())
        assert isinstance(handler, CustomHandler)

    def test_handler_in_handler_types(self):
        """Extra handlers appear in HandlerTypes for WorkerPool registration."""
        container = create_container(extra_handlers=[CustomHandler])
        handler_types = asyncio.run(container.get(HandlerTypes))

        assert CustomHandler in handler_types
        for core in _CORE_HANDLERS:
            assert core in handler_types

    def test_handler_in_subscription_registry(self):
        """Extra handlers are routed in the subscription registry."""
        container = create_container(extra_handlers=[CustomHandler])
        registry = asyncio.run(container.get(SubscriptionRegistry))

        assert "CustomEvent" in registry
        assert "CustomHandler" in registry["CustomEvent"]

    def test_no_extra_handlers_unchanged(self):
        """Without extra_handlers, only core handlers are present."""
        container = create_container()
        handler_types = asyncio.run(container.get(HandlerTypes))

        assert list(handler_types) == list(_CORE_HANDLERS)


# ---------------------------------------------------------------------------
# Tests: create_container — both extension points
# ---------------------------------------------------------------------------


class TestCombined:
    """Test providers and extra_handlers used simultaneously."""

    def test_both_providers_and_extra_handlers(self):
        """Provider overrides and extra handlers work together."""
        container = create_container(
            StubRunnerProvider(),
            extra_handlers=[CustomHandler],
        )

        async def resolve():
            handler_types = await container.get(HandlerTypes)
            async with container(scope=Scope.UOW) as uow:
                hook = await uow.get(HookRunner)
                custom = await uow.get(CustomHandler)
                return hook, custom, handler_types

        hook_runner, custom, handler_types = asyncio.run(resolve())

        assert isinstance(hook_runner, StubHookRunner)
        assert isinstance(custom, CustomHandler)
        assert CustomHandler in handler_types


# ---------------------------------------------------------------------------
# Tests: create_app
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _skip_handler_auth_validation():
    """Patch out validate_all_handlers for create_app tests.

    Other test modules define CommandHandler/QueryHandler subclasses without
    __auth__ declarations. Since __subclasses__() is process-wide, the startup
    validation picks them up and fails. This is orthogonal to what we test here.
    """
    with patch("osa.application.api.rest.app.validate_all_handlers"):
        yield


class TestCreateApp:
    """Test create_app passes extension points through to the container."""

    def test_provider_override_via_create_app(self):
        """Providers passed to create_app override default bindings."""
        app = create_app(providers=[StubRunnerProvider()])
        container = app.state.dishka_container

        async def resolve():
            async with container(scope=Scope.UOW) as uow:
                return await uow.get(HookRunner)

        runner = asyncio.run(resolve())
        assert isinstance(runner, StubHookRunner)

    def test_extra_handlers_via_create_app(self):
        """Extra handlers passed to create_app are wired in DI."""
        app = create_app(extra_handlers=[CustomHandler])
        container = app.state.dishka_container

        handler_types = asyncio.run(container.get(HandlerTypes))
        assert CustomHandler in handler_types

    def test_default_create_app_serves_health(self):
        """Default create_app produces a working app."""
        app = create_app()
        client = TestClient(app, raise_server_exceptions=False)
        response = client.get("/api/v1/health")
        assert response.status_code == 200
