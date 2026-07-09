"""Dependency-injection provider for telemetry instrumentation.

Binds the OTel meter (resolved from logfire's configured global MeterProvider)
and each instrumentation adapter as an APP-scoped singleton, so every service
shares one set of instruments. Trace sampling is configured process-wide in
:mod:`osa.infrastructure.telemetry.setup`, not here.
"""

from dishka import provide
from opentelemetry.metrics import Meter, get_meter_provider

from osa.domain.ingest.port.instrumentation import IngestInstrumentation
from osa.domain.shared.port.instrumentation import OutboxInstrumentation
from osa.domain.validation.port.instrumentation import HookInstrumentation
from osa.infrastructure.telemetry.api import ApiInstrumentation
from osa.infrastructure.telemetry.hook import OtelHookInstrumentation
from osa.infrastructure.telemetry.ingest import OtelIngestInstrumentation
from osa.infrastructure.telemetry.outbox import OtelOutboxInstrumentation
from osa.util.di.base import Provider
from osa.util.di.scope import Scope


class TelemetryProvider(Provider):
    """Provides the OTel meter and instrumentation adapters (all APP-scoped)."""

    @provide(scope=Scope.APP)
    def get_meter(self) -> Meter:
        """The application meter, from logfire's configured global MeterProvider."""
        return get_meter_provider().get_meter("osa")

    hook = provide(OtelHookInstrumentation, scope=Scope.APP, provides=HookInstrumentation)
    ingest = provide(OtelIngestInstrumentation, scope=Scope.APP, provides=IngestInstrumentation)
    outbox = provide(OtelOutboxInstrumentation, scope=Scope.APP, provides=OutboxInstrumentation)
    api = provide(ApiInstrumentation, scope=Scope.APP)
