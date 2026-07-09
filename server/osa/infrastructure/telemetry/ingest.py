"""OTel adapter implementing :class:`IngestInstrumentation`.

Owns the ``osa_ingest_*`` / ``osa_records_published_total`` metric family. The
``outcome`` label is drawn from a module-scoped enum (never free strings at the
call sites) and every other label from a bounded domain enum.
"""

from enum import StrEnum

from opentelemetry.metrics import Meter

from osa.domain.ingest.model.ingest_run import IngestStatus
from osa.domain.ingest.port.instrumentation import IngestInstrumentation
from osa.domain.shared.failure import FailureKind

_NO_KIND = "none"


class _BatchOutcome(StrEnum):
    """Bounded vocabulary for the ``outcome`` label on ``osa_ingest_batches_total``."""

    COMPLETED = "completed"
    FAILED = "failed"


class OtelIngestInstrumentation(IngestInstrumentation):
    """Emits ingest-run metrics through an injected OTel :class:`Meter`."""

    def __init__(self, meter: Meter) -> None:
        self._batches = meter.create_counter(
            "osa_ingest_batches_total",
            description="Count of ingest batches, by outcome (completed / failed).",
        )
        self._published = meter.create_counter(
            "osa_records_published_total",
            description="Count of records published by completed ingest batches.",
        )
        self._runs = meter.create_counter(
            "osa_ingest_runs_total",
            description="Count of ingest runs reaching a terminal status, by status and cause.",
        )

    def batch_completed(self, *, published_count: int) -> None:
        self._batches.add(1, {"outcome": _BatchOutcome.COMPLETED.value, "kind": _NO_KIND})
        self._published.add(published_count)

    def batch_failed(self, *, kind: FailureKind | None) -> None:
        self._batches.add(
            1,
            {
                "outcome": _BatchOutcome.FAILED.value,
                "kind": kind.value if kind is not None else _NO_KIND,
            },
        )

    def run_finished(self, *, status: IngestStatus, kind: FailureKind | None) -> None:
        self._runs.add(
            1,
            {"status": status.value, "kind": kind.value if kind is not None else _NO_KIND},
        )
