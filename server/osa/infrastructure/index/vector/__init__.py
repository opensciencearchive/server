"""Vector storage backend using ChromaDB and sentence-transformers."""

from osa.infrastructure.index.vector.backend import VectorStorageBackend
from osa.infrastructure.index.vector.config import (
    EmbeddingConfig,
    EmbeddingModel,
    VectorBackendConfig,
)

__all__ = [
    "VectorStorageBackend",
    "VectorBackendConfig",
    "EmbeddingConfig",
    "EmbeddingModel",
]
