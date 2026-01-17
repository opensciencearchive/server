"""Index SDK - Protocols and types for pluggable storage backends."""

from osa.sdk.index.backend import StorageBackend
from osa.sdk.index.config import BackendConfig
from osa.sdk.index.result import QueryResult, SearchHit

__all__ = [
    "StorageBackend",
    "BackendConfig",
    "QueryResult",
    "SearchHit",
]
