"""Process-wide telemetry bootstrap (metrics + logs + traces via Logfire).

Extracts the inline ``logfire.configure(...)`` block that used to live in
``app.py`` and extends it with OSA's observability config: an owned Prometheus
pull registry, optional OTLP/HTTP push of all three signals, and head sampling.

Why a module singleton (:data:`bootstrap`): logfire's provider wiring and
Prometheus collector registration are *process*-global. Test suites build many
apps per process via ``create_app()``; re-running ``logfire.configure`` or
re-registering a Prometheus collector would corrupt global state or raise
``Duplicated timeseries``. :class:`TelemetryBootstrap` guards that with a
configure-once flag and an owned :class:`CollectorRegistry` (never the
prometheus_client default ``REGISTRY``).
"""

import logging
import sys

import logfire
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk._logs import LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import Histogram
from opentelemetry.sdk.metrics.export import MetricReader, PeriodicExportingMetricReader
from opentelemetry.sdk.metrics.view import (
    ExplicitBucketHistogramAggregation,
    ExponentialBucketHistogramAggregation,
    View,
)
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor, SpanProcessor
from prometheus_client import REGISTRY as _PROMETHEUS_DEFAULT_REGISTRY
from prometheus_client import CollectorRegistry

from osa.config import Config
from osa.infrastructure.logging import OSAConsoleExporter, get_logger

logger = get_logger(__name__)


class _OwnedRegistryPrometheusReader(PrometheusMetricReader):
    """A :class:`PrometheusMetricReader` bound to an *owned* CollectorRegistry.

    ``PrometheusMetricReader`` (exporter-prometheus 0.60b1) has no registry
    kwarg: ``__init__`` unconditionally registers its collector into the
    process-global default ``REGISTRY``, and ``shutdown`` unregisters it from
    the same. We want an owned registry instead (so ``/metrics`` renders only
    OSA's collector, never the global default's process/platform collectors),
    so the cleanest supported path is to let the base register into the default,
    then relocate the collector into the owned registry — and mirror that in an
    overridden ``shutdown`` so teardown unregisters from the right registry (no
    KeyError against the global default at process exit).
    """

    def __init__(self, registry: CollectorRegistry) -> None:
        self._owned_registry = registry
        super().__init__()
        _PROMETHEUS_DEFAULT_REGISTRY.unregister(self._collector)
        registry.register(self._collector)

    def shutdown(self, timeout_millis: float = 30_000, **kwargs: object) -> None:
        self._owned_registry.unregister(self._collector)


class TelemetryBootstrap:
    """Process-wide telemetry configuration. Idempotent: configure() runs once per process."""

    def __init__(self) -> None:
        self._configured = False
        self._prometheus_registry: CollectorRegistry | None = None

    @property
    def prometheus_registry(self) -> CollectorRegistry | None:
        """The owned Prometheus registry ``/metrics`` renders, or None when disabled."""
        return self._prometheus_registry

    @staticmethod
    def _metric_views(*, prometheus_enabled: bool) -> list[View]:
        """Logfire's default views, made Prometheus-compatible when needed.

        With the pull endpoint off, logfire's defaults stand (exponential
        histograms — better resolution, smaller payloads). With it on, the
        exponential-aggregation view is replaced by explicit buckets, because
        prometheus-exporter 0.60b1 raises ``AttributeError`` on
        ``ExponentialHistogramDataPoint`` during collection. The other default
        views (e.g. ``http.server.active_requests`` attribute filtering) are
        kept either way.
        """
        defaults = list(logfire.MetricsOptions.DEFAULT_VIEWS)
        if not prometheus_enabled:
            return defaults
        views = [
            v
            for v in defaults
            if not isinstance(
                getattr(v, "_aggregation", None), ExponentialBucketHistogramAggregation
            )
        ]
        views.append(
            View(instrument_type=Histogram, aggregation=ExplicitBucketHistogramAggregation())
        )
        return views

    def configure(self, config: Config) -> None:
        """Configure the process-global telemetry pipeline exactly once.

        Reproduces the original ``app.py`` logfire block (console span exporter,
        stdlib-root → Logfire handler bridge, uvicorn.access suppression) and
        extends it with service version, head sampling, an owned Prometheus
        reader, and — when an OTLP endpoint is set — OTLP/HTTP push of traces,
        metrics, and logs.
        """
        if self._configured:
            logger.debug("Telemetry already configured for this process; skipping.")
            return

        obs = config.observability

        # Base span processors: the console exporter is always present (parity
        # with the original app.py block).
        span_processors: list[SpanProcessor] = [
            SimpleSpanProcessor(
                OSAConsoleExporter(
                    output=sys.stderr,
                    include_timestamp=True,
                    min_log_level=config.logging.level,
                )
            ),
        ]
        metric_readers: list[MetricReader] = []
        log_processors: list[BatchLogRecordProcessor] = []

        if obs.prometheus_enabled:
            self._prometheus_registry = CollectorRegistry()
            metric_readers.append(_OwnedRegistryPrometheusReader(self._prometheus_registry))

        if obs.otlp_endpoint is not None:
            base = obs.otlp_endpoint.rstrip("/")
            headers = (
                {"Authorization": f"Bearer {obs.otlp_token.get_secret_value()}"}
                if obs.otlp_token is not None
                else None
            )
            span_processors.append(
                BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{base}/v1/traces", headers=headers))
            )
            metric_readers.append(
                PeriodicExportingMetricReader(
                    OTLPMetricExporter(endpoint=f"{base}/v1/metrics", headers=headers)
                )
            )
            log_processors.append(
                BatchLogRecordProcessor(
                    OTLPLogExporter(endpoint=f"{base}/v1/logs", headers=headers)
                )
            )

        advanced = (
            logfire.AdvancedOptions(log_record_processors=list(log_processors))
            if log_processors
            else None
        )

        # Logfire's default views aggregate all histograms into exponential
        # buckets, which the Prometheus exporter cannot render (its collector
        # crashes on ExponentialHistogramDataPoint at scrape time). Views are
        # provider-wide, so when the pull endpoint is on we fall back to
        # explicit buckets for every histogram — OTLP consumers handle both.
        views = self._metric_views(prometheus_enabled=obs.prometheus_enabled)

        logfire.configure(
            send_to_logfire="if-token-present",
            service_name=config.name,
            service_version=config.version,
            console=False,  # we render spans via OSAConsoleExporter
            inspect_arguments=False,
            additional_span_processors=span_processors,
            metrics=logfire.MetricsOptions(additional_readers=list(metric_readers), views=views),
            sampling=logfire.SamplingOptions(head=obs.trace_sample_rate),
            advanced=advanced,
        )

        # Route stdlib logging through logfire so legacy logger.info() calls land
        # in the same stream. When OTLP is on, also feed the stdlib root into the
        # OTLP logs signal so those records reach the collector.
        root = logging.getLogger()
        root.setLevel(config.logging.level.upper())
        for handler in root.handlers[:]:
            root.removeHandler(handler)
        root.addHandler(logfire.LogfireLoggingHandler())
        if log_processors:
            root.addHandler(LoggingHandler())

        # logfire's FastAPI instrumentation already emits HTTP access telemetry.
        logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

        # The MCP SDK narrates its per-request transport lifecycle at INFO
        # ("Processing request of type CallToolRequest", "Terminating session:
        # None" — the latter is just stateless mode tearing down its throwaway
        # per-request session). Our own osa.api.mcp logger carries the useful
        # tool-call lines, so quiet the SDK's play-by-play.
        logging.getLogger("mcp").setLevel(logging.WARNING)

        logger.info(
            "Telemetry configured",
            prometheus_enabled=obs.prometheus_enabled,
            otlp_endpoint=obs.otlp_endpoint,  # never the token
            trace_sample_rate=obs.trace_sample_rate,
        )
        self._configured = True


bootstrap = TelemetryBootstrap()
"""Module singleton — telemetry is process-global by nature (like logfire itself)."""
