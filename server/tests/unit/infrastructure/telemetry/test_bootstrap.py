"""Unit tests for TelemetryBootstrap.

logfire.configure is monkeypatched to a recording stub so we can assert what the
bootstrap *would* configure (readers, span/log processors, exporter endpoints
and headers) without touching process-global provider state.
"""

from typing import Any

import pytest
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from pydantic import SecretStr

from osa.config import Config
from osa.infrastructure.telemetry import setup as setup_mod
from osa.infrastructure.telemetry.setup import TelemetryBootstrap


class _Recorder:
    """Records the kwargs passed to logfire.configure."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def __call__(self, **kwargs: Any) -> None:
        self.calls.append(kwargs)


@pytest.fixture
def recorder(monkeypatch: pytest.MonkeyPatch) -> _Recorder:
    rec = _Recorder()
    monkeypatch.setattr(setup_mod.logfire, "configure", rec)
    # LogfireLoggingHandler() must be constructible without a real configure.
    return rec


def _config(**observability: Any) -> Config:
    return Config(
        base_url="http://localhost:8000",
        observability=observability,  # type: ignore[arg-type]  # pydantic coerces dict
    )


def _span_exporter(processor: BatchSpanProcessor):
    return processor._batch_processor._exporter  # noqa: SLF001


def _log_exporter(processor: BatchLogRecordProcessor):
    return processor._batch_processor._exporter  # noqa: SLF001


def _metric_exporter(reader: PeriodicExportingMetricReader):
    return reader._exporter  # noqa: SLF001


def test_configure_is_idempotent(recorder: _Recorder) -> None:
    boot = TelemetryBootstrap()
    boot.configure(_config())
    boot.configure(_config())
    assert len(recorder.calls) == 1


def test_prometheus_disabled_no_reader_no_registry(recorder: _Recorder) -> None:
    boot = TelemetryBootstrap()
    boot.configure(_config(prometheus_enabled=False))

    readers = recorder.calls[0]["metrics"].additional_readers
    assert not any(isinstance(r, PrometheusMetricReader) for r in readers)
    assert boot.prometheus_registry is None


def test_prometheus_enabled_owns_registry(recorder: _Recorder) -> None:
    boot = TelemetryBootstrap()
    boot.configure(_config(prometheus_enabled=True))

    readers = recorder.calls[0]["metrics"].additional_readers
    assert any(isinstance(r, PrometheusMetricReader) for r in readers)
    assert boot.prometheus_registry is not None


def test_sampling_head_rate_passed_through(recorder: _Recorder) -> None:
    boot = TelemetryBootstrap()
    boot.configure(_config(trace_sample_rate=0.25))
    assert recorder.calls[0]["sampling"].head == pytest.approx(0.25)


def test_no_otlp_means_no_advanced_and_no_push_exporters(recorder: _Recorder) -> None:
    boot = TelemetryBootstrap()
    boot.configure(_config(prometheus_enabled=False))
    call = recorder.calls[0]

    assert call["advanced"] is None
    assert not any(isinstance(p, BatchSpanProcessor) for p in call["additional_span_processors"])
    assert call["metrics"].additional_readers == []


def test_otlp_endpoint_wires_all_three_signals_with_auth(recorder: _Recorder) -> None:
    boot = TelemetryBootstrap()
    boot.configure(
        _config(
            otlp_endpoint="https://collector.example/",  # trailing slash must be stripped
            otlp_token=SecretStr("s3cret"),
            prometheus_enabled=False,
        )
    )
    call = recorder.calls[0]

    # Traces
    span_procs = [
        p for p in call["additional_span_processors"] if isinstance(p, BatchSpanProcessor)
    ]
    assert len(span_procs) == 1
    span_exp = _span_exporter(span_procs[0])
    assert span_exp._endpoint == "https://collector.example/v1/traces"  # noqa: SLF001
    assert span_exp._session.headers["Authorization"] == "Bearer s3cret"  # noqa: SLF001

    # Metrics
    metric_readers = [
        r
        for r in call["metrics"].additional_readers
        if isinstance(r, PeriodicExportingMetricReader)
    ]
    assert len(metric_readers) == 1
    metric_exp = _metric_exporter(metric_readers[0])
    assert metric_exp._endpoint == "https://collector.example/v1/metrics"  # noqa: SLF001
    assert metric_exp._session.headers["Authorization"] == "Bearer s3cret"  # noqa: SLF001

    # Logs
    log_procs = call["advanced"].log_record_processors
    assert len(log_procs) == 1
    log_exp = _log_exporter(log_procs[0])
    assert log_exp._endpoint == "https://collector.example/v1/logs"  # noqa: SLF001
    assert log_exp._session.headers["Authorization"] == "Bearer s3cret"  # noqa: SLF001


def test_otlp_without_token_sends_no_auth_header(recorder: _Recorder) -> None:
    boot = TelemetryBootstrap()
    boot.configure(_config(otlp_endpoint="https://collector.example", prometheus_enabled=False))
    call = recorder.calls[0]
    span_procs = [
        p for p in call["additional_span_processors"] if isinstance(p, BatchSpanProcessor)
    ]
    span_exp = _span_exporter(span_procs[0])
    assert "Authorization" not in span_exp._session.headers  # noqa: SLF001


# ── Prometheus-compatible histogram views (regression: #158 smoke test) ──────
#
# prometheus-exporter 0.60b1 crashes on ExponentialHistogramDataPoint at
# collection time, and logfire's DEFAULT_VIEWS aggregate every histogram
# exponentially. The bootstrap must swap in explicit buckets whenever the pull
# endpoint is enabled — otherwise the first scrape after any histogram record
# (e.g. one instrumented HTTP request) returns a 500.


def test_views_keep_logfire_defaults_when_prometheus_disabled() -> None:
    views = TelemetryBootstrap._metric_views(prometheus_enabled=False)
    assert views == list(setup_mod.logfire.MetricsOptions.DEFAULT_VIEWS)


def test_views_replace_exponential_histograms_when_prometheus_enabled() -> None:
    from opentelemetry.sdk.metrics.view import ExponentialBucketHistogramAggregation

    views = TelemetryBootstrap._metric_views(prometheus_enabled=True)
    assert not any(
        isinstance(getattr(v, "_aggregation", None), ExponentialBucketHistogramAggregation)
        for v in views
    )
    # The non-histogram defaults (active_requests attribute filtering) survive.
    assert len(views) == len(setup_mod.logfire.MetricsOptions.DEFAULT_VIEWS)


def test_histogram_records_render_through_prometheus_reader() -> None:
    """End-to-end: a recorded histogram must survive a Prometheus collection."""
    from opentelemetry.sdk.metrics import MeterProvider
    from prometheus_client import CollectorRegistry, generate_latest

    from osa.infrastructure.telemetry.setup import _OwnedRegistryPrometheusReader

    registry = CollectorRegistry()
    reader = _OwnedRegistryPrometheusReader(registry)
    provider = MeterProvider(
        metric_readers=[reader],
        views=TelemetryBootstrap._metric_views(prometheus_enabled=True),
    )
    try:
        meter = provider.get_meter("osa-test")
        histogram = meter.create_histogram("osa_test_duration_seconds", unit="s")
        histogram.record(0.25, {"hook": "demo"})

        exposition = generate_latest(registry).decode()
        assert "osa_test_duration_seconds" in exposition
    finally:
        provider.shutdown()
