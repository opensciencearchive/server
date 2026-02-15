"""Vector storage backend using ChromaDB and sentence-transformers."""

import asyncio
import logging
from typing import Any

import chromadb
from sentence_transformers import SentenceTransformer

from osa.infrastructure.index.vector.config import VectorBackendConfig
from osa.sdk.index.result import QueryResult, SearchHit

logger = logging.getLogger(__name__)


class VectorStorageBackend:
    """Vector similarity backend using ChromaDB + sentence-transformers.

    All blocking operations (embedding generation, ChromaDB I/O) are run
    in a thread pool to avoid blocking the async event loop.

    This backend is stateless - batching is handled at the event level by
    the BackgroundWorker. Use ingest_batch() for efficient batch operations.
    """

    def __init__(self, name: str, config: VectorBackendConfig) -> None:
        self._name = name
        self._config = config
        self._model = SentenceTransformer(config.embedding.model.value)

        # persist_dir must be set by DI (derived from OSAPaths if not explicit)
        if config.persist_dir is None:
            raise ValueError("VectorBackendConfig.persist_dir must be set")

        # Expand ~ to home directory and ensure persist directory exists
        persist_dir = config.persist_dir.expanduser()
        persist_dir.mkdir(parents=True, exist_ok=True)

        self._client = chromadb.PersistentClient(path=str(persist_dir))
        self._collection = self._client.get_or_create_collection(
            name=name,
            metadata={"hnsw:space": "cosine"},
        )

    @property
    def name(self) -> str:
        return self._name

    async def ingest(self, srn: str, record: dict[str, Any]) -> None:
        """Index a single record.

        Delegates to ingest_batch() for consistent behavior.
        For efficiency, prefer using ingest_batch() directly when
        processing multiple records.
        """
        await self.ingest_batch([(srn, record)])

    async def ingest_batch(self, records: list[tuple[str, dict[str, Any]]]) -> None:
        """Index a batch of records atomically with batch embedding generation.

        Generates embeddings for all records in a single batch call to
        the embedding model, then bulk upserts to ChromaDB.

        Args:
            records: List of (srn, metadata) tuples to index

        Raises:
            Exception: If any record fails to index (none are committed)
        """
        if not records:
            return

        # Prepare batch data, skipping records that have no indexable content
        ids = []
        texts = []
        metadatas = []
        skipped = 0

        for srn, record in records:
            text = self._to_text(record)
            safe_meta = {k: v for k, v in record.items() if isinstance(v, (str, int, float, bool))}

            if not text.strip() and not safe_meta:
                logger.warning(f"Skipping record {srn}: no indexable content for vector backend")
                skipped += 1
                continue

            ids.append(srn)
            texts.append(text)
            metadatas.append(safe_meta if safe_meta else {"srn": srn})

        if not ids:
            logger.warning(f"All {skipped} records skipped: no indexable content")
            return

        # Generate embeddings in batch (much more efficient than one-by-one)
        logger.debug(f"Generating embeddings for {len(texts)} records")
        embeddings = await asyncio.to_thread(lambda: self._model.encode(texts).tolist())

        # Bulk upsert to ChromaDB
        await asyncio.to_thread(
            self._collection.upsert,
            ids=ids,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=texts,
        )

        indexed = len(ids)
        msg = f"Indexed {indexed} records"
        if skipped:
            msg += f" ({skipped} skipped — metadata fields don't match index config)"
        logger.info(msg)

    async def flush(self) -> None:
        """No-op: batching is now handled at the event level.

        Deprecated: This method is retained for backward compatibility
        but does nothing. Batching is handled by the BackgroundWorker
        using the outbox as a durable buffer.
        """
        pass

    async def delete(self, srn: str) -> None:
        """Remove a record from the index."""
        await asyncio.to_thread(self._collection.delete, ids=[srn])

    async def query(self, q: str, limit: int = 20) -> QueryResult:
        """Execute a query and return structured results."""
        # Run CPU-bound embedding in thread pool and convert to list
        embedding = await asyncio.to_thread(lambda: self._model.encode(q).tolist())

        # Run ChromaDB query in thread pool
        results = await asyncio.to_thread(
            self._collection.query,
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
            await asyncio.to_thread(self._collection.count)
            return True
        except Exception as e:
            logger.warning(f"Health check failed for backend '{self._name}': {e}")
            return False

    async def count(self) -> int:
        """Return the number of documents in the index."""
        return await asyncio.to_thread(self._collection.count)

    def _to_text(self, record: dict[str, Any]) -> str:
        """Convert record to embeddable text."""
        if self._config.embedding.template:
            return self._config.embedding.template.format(**record)
        return " ".join(str(record.get(f, "")) for f in self._config.embedding.fields)
