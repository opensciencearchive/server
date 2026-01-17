"""Source SDK - Protocols and types for pluggable data sources."""

from osa.sdk.source.config import SourceConfig
from osa.sdk.source.record import UpstreamRecord
from osa.sdk.source.source import Source

__all__ = [
    "Source",
    "SourceConfig",
    "UpstreamRecord",
]
