"""Unit tests for the OTel instrumentation adapters.

Pure OpenTelemetry — no logfire. Each adapter is driven against an in-memory
meter provider, then the exported metric points are asserted for name, labels,
and value. This is the red-first spec for the adapters.
"""

from collections.abc import Iterator

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    Histogram,
    InMemoryMetricReader,
    Metric,
    Sum,
)

from osa.domain.ingest.model.ingest_run import IngestStatus
from osa.domain.shared.event import DeliveryStatus
from osa.domain.shared.failure import DecisionKind, FailureKind
from osa.domain.shared.model.hook import HookName
from osa.domain.shared.model.workflow import StageOutcome, WorkflowName, WorkflowStage
from osa.domain.validation.model.hook_run import HookRunStatus
from osa.infrastructure.telemetry.api import ApiInstrumentation
from osa.infrastructure.telemetry.hook import OtelHookInstrumentation
from osa.infrastructure.telemetry.ingest import OtelIngestInstrumentation
from osa.infrastructure.telemetry.outbox import OtelOutboxInstrumentation
from osa.infrastructure.telemetry.workflow import OtelWorkflowInstrumentation


@pytest.fixture
def reader() -> InMemoryMetricReader:
    return InMemoryMetricReader()


@pytest.fixture
def meter(reader: InMemoryMetricReader):
    provider = MeterProvider(metric_readers=[reader])
    return provider.get_meter("test")


def _metrics(reader: InMemoryMetricReader) -> Iterator[Metric]:
    data = reader.get_metrics_data()
    if data is None:
        return
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            yield from sm.metrics


def _by_name(reader: InMemoryMetricReader, name: str) -> Metric:
    for metric in _metrics(reader):
        if metric.name == name:
            return metric
    raise AssertionError(f"metric {name!r} not exported; saw {[m.name for m in _metrics(reader)]}")


def _points(reader: InMemoryMetricReader, name: str) -> list[tuple[dict[str, object], object]]:
    metric = _by_name(reader, name)
    assert isinstance(metric.data, (Sum, Histogram))
    return [(dict(p.attributes), p) for p in metric.data.data_points]


# ── Hook adapter ──────────────────────────────────────────────────────────────


def test_hook_run_finished_counts_by_hook_and_status(reader, meter):
    instr = OtelHookInstrumentation(meter)
    hook = HookName("qc")

    instr.run_finished(hook=hook, status=HookRunStatus.PASSED, duration_s=1.5, oom_retries=0)
    instr.run_finished(hook=hook, status=HookRunStatus.FAILED, duration_s=0.5, oom_retries=0)

    points = {tuple(sorted(a.items())): p.value for a, p in _points(reader, "osa_hook_runs_total")}
    assert points[(("hook", "qc"), ("status", "passed"))] == 1
    assert points[(("hook", "qc"), ("status", "failed"))] == 1


def test_hook_duration_histogram_records_sum(reader, meter):
    instr = OtelHookInstrumentation(meter)
    instr.run_finished(
        hook=HookName("qc"), status=HookRunStatus.PASSED, duration_s=2.0, oom_retries=0
    )

    (attrs, point) = _points(reader, "osa_hook_run_duration_seconds")[0]
    assert attrs == {"hook": "qc", "status": "passed"}
    assert point.sum == pytest.approx(2.0)


def test_hook_oom_retries_only_emitted_when_positive(reader, meter):
    instr = OtelHookInstrumentation(meter)
    instr.run_finished(
        hook=HookName("qc"), status=HookRunStatus.PASSED, duration_s=1.0, oom_retries=0
    )
    # No datapoint yet — counter never touched.
    with pytest.raises(AssertionError):
        _by_name(reader, "osa_hook_oom_retries_total")

    instr.run_finished(
        hook=HookName("qc"), status=HookRunStatus.PASSED, duration_s=1.0, oom_retries=3
    )
    (attrs, point) = _points(reader, "osa_hook_oom_retries_total")[0]
    assert attrs == {"hook": "qc"}
    assert point.value == 3


def test_hook_failure_decided_labels(reader, meter):
    instr = OtelHookInstrumentation(meter)
    instr.run_failure_decided(
        hook=HookName("qc"), kind=FailureKind.OOM, decision=DecisionKind.RETRY_WITH_MORE_MEMORY
    )
    (attrs, point) = _points(reader, "osa_hook_failures_total")[0]
    assert attrs == {"hook": "qc", "kind": "oom", "decision": "retry_with_more_memory"}
    assert point.value == 1


# ── Ingest adapter ────────────────────────────────────────────────────────────


def test_ingest_batch_completed_counts_and_publishes(reader, meter):
    instr = OtelIngestInstrumentation(meter)
    instr.batch_completed(published_count=42)

    (attrs, point) = _points(reader, "osa_ingest_batches_total")[0]
    assert attrs == {"outcome": "completed", "kind": "none"}
    assert point.value == 1

    (_, published) = _points(reader, "osa_records_published_total")[0]
    assert published.value == 42


def test_ingest_batch_failed_outcome_label(reader, meter):
    instr = OtelIngestInstrumentation(meter)
    instr.batch_failed(kind=FailureKind.UPSTREAM)

    (attrs, point) = _points(reader, "osa_ingest_batches_total")[0]
    assert attrs == {"outcome": "failed", "kind": "upstream"}
    assert point.value == 1


def test_ingest_run_finished_kind_none_maps_to_none_label(reader, meter):
    instr = OtelIngestInstrumentation(meter)
    instr.run_finished(status=IngestStatus.COMPLETED, kind=None)
    instr.run_finished(status=IngestStatus.FAILED, kind=FailureKind.IMAGE_PULL)

    points = {
        tuple(sorted(a.items())): p.value for a, p in _points(reader, "osa_ingest_runs_total")
    }
    assert points[(("kind", "none"), ("status", "completed"))] == 1
    assert points[(("kind", "image_pull"), ("status", "failed"))] == 1


# ── Outbox adapter ────────────────────────────────────────────────────────────


def test_outbox_delivery_completed_counter_and_histogram(reader, meter):
    instr = OtelOutboxInstrumentation(meter)
    instr.delivery_completed(
        consumer_group="RunHooks", status=DeliveryStatus.DELIVERED, retry_count=0, duration_s=0.25
    )

    (attrs, point) = _points(reader, "osa_deliveries_total")[0]
    assert attrs == {"consumer_group": "RunHooks", "status": "delivered"}
    assert point.value == 1

    (h_attrs, h_point) = _points(reader, "osa_dispatch_duration_seconds")[0]
    assert h_attrs == {"consumer_group": "RunHooks"}
    assert h_point.sum == pytest.approx(0.25)


# ── Workflow adapter ──────────────────────────────────────────────────────────


def test_workflow_stage_finished_counts_by_workflow_stage_outcome(reader, meter):
    instr = OtelWorkflowInstrumentation(meter)

    instr.stage_finished(
        workflow=WorkflowName.PROCESS_SUBMISSION,
        stage=WorkflowStage.VALIDATE,
        outcome=StageOutcome.RAN,
    )
    instr.stage_finished(
        workflow=WorkflowName.PROCESS_BATCH,
        stage=WorkflowStage.HOOKS,
        outcome=StageOutcome.SKIPPED,
    )

    points = {
        tuple(sorted(a.items())): p.value for a, p in _points(reader, "osa_workflow_stages_total")
    }
    assert points[(("outcome", "ran"), ("stage", "validate"), ("workflow", "process_submission"))] == 1
    assert points[(("outcome", "skipped"), ("stage", "hooks"), ("workflow", "process_batch"))] == 1


def test_workflow_stage_finished_exact_label_dict(reader, meter):
    instr = OtelWorkflowInstrumentation(meter)

    instr.stage_finished(
        workflow=WorkflowName.PROCESS_BATCH,
        stage=WorkflowStage.INSERT_FEATURES,
        outcome=StageOutcome.FAILED,
    )

    (attrs, point) = _points(reader, "osa_workflow_stages_total")[0]
    assert attrs == {
        "workflow": "process_batch",
        "stage": "insert_features",
        "outcome": "failed",
    }
    assert point.value == 1


# ── API adapter ───────────────────────────────────────────────────────────────


def test_api_unhandled_error_counter(reader, meter):
    instr = ApiInstrumentation(meter)
    instr.unhandled_error()
    instr.unhandled_error()

    (_, point) = _points(reader, "osa_unhandled_errors_total")[0]
    assert point.value == 2
