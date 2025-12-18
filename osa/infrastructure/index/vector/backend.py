"""Vector storage backend using ChromaDB and sentence-transformers."""

from typing import Any

import chromadb
from sentence_transformers import SentenceTransformer

from osa.infrastructure.index.vector.config import VectorBackendConfig
from osa.sdk.index.result import QueryResult, SearchHit


class VectorStorageBackend:
    """Vector similarity backend using ChromaDB + sentence-transformers."""

    def __init__(self, name: str, config: VectorBackendConfig) -> None:
        self._name = name
        self._config = config
        self._model = SentenceTransformer(config.embedding.model.value)

        # Ensure persist directory exists
        config.persist_dir.mkdir(parents=True, exist_ok=True)

        self._client = chromadb.PersistentClient(path=str(config.persist_dir))
        self._collection = self._client.get_or_create_collection(
            name=name,
            metadata={"hnsw:space": "cosine"},
        )

    @property
    def name(self) -> str:
        return self._name

    async def ingest(self, srn: str, record: dict[str, Any]) -> None:
        """Store a record in the index."""
        text = self._to_text(record)
        embedding = self._model.encode(text).tolist()

        # Filter metadata to ChromaDB-compatible types
        safe_meta = {
            k: v
            for k, v in record.items()
            if isinstance(v, (str, int, float, bool))
        }

        self._collection.upsert(
            ids=[srn],
            embeddings=[embedding],
            metadatas=[safe_meta],
            documents=[text],
        )

    async def delete(self, srn: str) -> None:
        """Remove a record from the index."""
        self._collection.delete(ids=[srn])

    async def query(self, q: str, limit: int = 20) -> QueryResult:
        """Execute a query and return structured results."""
        embedding = self._model.encode(q).tolist()
        results = self._collection.query(
            query_embeddings=[embedding],
            n_results=limit,
            include=["metadatas", "distances"],
        )

        hits = []
        ids = results["ids"][0] if results["ids"] else []
        distances = results["distances"][0] if results["distances"] else []
        metadatas = results["metadatas"][0] if results["metadatas"] else []

        for i, srn in enumerate(ids):
            # Convert ChromaDB metadata to dict[str, Any]
            meta: dict[str, Any] = dict(metadatas[i]) if i < len(metadatas) else {}
            hits.append(
                SearchHit(
                    srn=srn,
                    score=1 - distances[i] if i < len(distances) else 0.0,
                    metadata=meta,
                )
            )

        return QueryResult(hits=hits, total=len(hits), query=q)

    async def health(self) -> bool:
        """Check if the backend is operational."""
        try:
            self._collection.count()
            return True
        except Exception:
            return False

    def _to_text(self, record: dict[str, Any]) -> str:
        """Convert record to embeddable text."""
        if self._config.embedding.template:
            return self._config.embedding.template.format(**record)
        return " ".join(
            str(record.get(f, "")) for f in self._config.embedding.fields
        )
