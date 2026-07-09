"""Unit tests for TelemetryProvider DI wiring.

The container resolves each instrumentation port to its OTel adapter and the
Meter as an APP-scoped singleton.
"""

import asyncio
import os

from opentelemetry.metrics import Meter

os.environ.setdefault("OSA_AUTH__JWT__SECRET", "test-secret-that-is-at-least-32-characters-long")
os.environ.setdefault("OSA_BASE_URL", "http://localhost:8000")

from osa.application.di import create_container  # noqa: E402
from osa.domain.ingest.port.instrumentation import IngestInstrumentation  # noqa: E402
from osa.domain.shared.port.instrumentation import OutboxInstrumentation  # noqa: E402
from osa.domain.validation.port.instrumentation import HookInstrumentation  # noqa: E402
from osa.infrastructure.telemetry.api import ApiInstrumentation  # noqa: E402
from osa.infrastructure.telemetry.hook import OtelHookInstrumentation  # noqa: E402
from osa.infrastructure.telemetry.ingest import OtelIngestInstrumentation  # noqa: E402
from osa.infrastructure.telemetry.outbox import OtelOutboxInstrumentation  # noqa: E402


def test_ports_resolve_to_otel_adapters() -> None:
    container = create_container()

    async def resolve():
        hook = await container.get(HookInstrumentation)
        ingest = await container.get(IngestInstrumentation)
        outbox = await container.get(OutboxInstrumentation)
        api = await container.get(ApiInstrumentation)
        meter = await container.get(Meter)
        return hook, ingest, outbox, api, meter

    hook, ingest, outbox, api, meter = asyncio.run(resolve())

    assert isinstance(hook, OtelHookInstrumentation)
    assert isinstance(ingest, OtelIngestInstrumentation)
    assert isinstance(outbox, OtelOutboxInstrumentation)
    assert isinstance(api, ApiInstrumentation)
    assert isinstance(meter, Meter)


def test_instrumentation_is_app_scoped_singleton() -> None:
    container = create_container()

    async def resolve():
        first = await container.get(HookInstrumentation)
        second = await container.get(HookInstrumentation)
        return first, second

    first, second = asyncio.run(resolve())
    assert first is second
