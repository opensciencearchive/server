"""Configuration for ChromaDB + sentence-transformers vector backend."""

from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel

from osa.sdk.index.config import BackendConfig


class EmbeddingModel(StrEnum):
    """Supported embedding models from sentence-transformers."""

    # Lightweight, fast models
    MINILM_L6 = "all-MiniLM-L6-v2"
    MINILM_L12 = "all-MiniLM-L12-v2"

    # Higher quality models
    MPNET_BASE = "all-mpnet-base-v2"

    # Biomedical domain
    PUBMEDBERT = "pritamdeka/S-PubMedBert-MS-MARCO"


class EmbeddingConfig(BaseModel):
    """Configuration for text embedding."""

    model: EmbeddingModel = EmbeddingModel.MINILM_L6
    fields: list[str] = ["title", "summary"]
    template: str | None = None  # Optional: "{title}. {summary}"


class VectorBackendConfig(BackendConfig):
    """ChromaDB + sentence-transformers specific configuration."""

    persist_dir: Path
    embedding: EmbeddingConfig = EmbeddingConfig()
