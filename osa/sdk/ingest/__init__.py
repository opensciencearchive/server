"""Ingest SDK - Protocols and types for pluggable data source ingestors."""

from osa.sdk.ingest.config import IngestorConfig
from osa.sdk.ingest.ingestor import Ingestor
from osa.sdk.ingest.record import UpstreamRecord

__all__ = [
    "Ingestor",
    "IngestorConfig",
    "UpstreamRecord",
]
