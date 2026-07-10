"""IngestInstrumentation port — a domain-probe for ingest-run telemetry.

One method per business fact worth measuring (a batch completed / failed; a run
reached a terminal state). Implemented by infrastructure (an OTel adapter);
trivially no-op-able in tests. All methods are synchronous — metric emission
must never block ingestion — and keyword-only so call sites read as named
facts. Labels are typed enums so metric cardinality stays bounded.
"""

from abc import abstractmethod
from typing import Protocol

from osa.domain.ingest.model.ingest_run import IngestStatus
from osa.domain.shared.failure import FailureKind
from osa.domain.shared.port import Port


class IngestInstrumentation(Port, Protocol):
    """Domain-probe for ingest-run metrics (see module docstring)."""

    @abstractmethod
    def batch_completed(self, *, published_count: int) -> None:
        """Record a batch that completed, publishing ``published_count`` records."""
        ...

    @abstractmethod
    def batch_failed(self, *, kind: FailureKind | None) -> None:
        """Record a batch that failed; ``kind`` is the observed cause when known."""
        ...

    @abstractmethod
    def run_finished(self, *, status: IngestStatus, kind: FailureKind | None) -> None:
        """Record an ingest run reaching a terminal status (completed / failed)."""
        ...
