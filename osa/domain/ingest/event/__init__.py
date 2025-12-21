"""Ingest domain events."""

from osa.domain.ingest.event.ingest_requested import IngestRequested
from osa.domain.ingest.event.ingestion_run_completed import IngestionRunCompleted

__all__ = ["IngestRequested", "IngestionRunCompleted"]
