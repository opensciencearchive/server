"""Unit tests for VectorStorageBackend.ingest_batch()."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from osa.infrastructure.index.vector.backend import VectorStorageBackend
from osa.infrastructure.index.vector.config import EmbeddingConfig, VectorBackendConfig


@pytest.fixture
def mock_sentence_transformer():
    """Create a mock SentenceTransformer."""
    mock_model = MagicMock()
    # encode returns a mock array with tolist method
    mock_embedding = MagicMock()
    mock_embedding.tolist.return_value = [[0.1, 0.2, 0.3]]
    mock_model.encode.return_value = mock_embedding
    return mock_model


@pytest.fixture
def mock_chroma_client():
    """Create a mock ChromaDB client."""
    mock_collection = MagicMock()
    mock_collection.upsert = MagicMock()
    mock_collection.count = MagicMock(return_value=0)

    mock_client = MagicMock()
    mock_client.get_or_create_collection.return_value = mock_collection
    return mock_client, mock_collection


@pytest.fixture
def vector_backend(tmp_path: Path, mock_sentence_transformer, mock_chroma_client):
    """Create a VectorStorageBackend with mocked dependencies."""
    mock_client, mock_collection = mock_chroma_client

    config = VectorBackendConfig(
        persist_dir=tmp_path,
        embedding=EmbeddingConfig(fields=["title", "abstract"]),
    )

    with patch(
        "osa.infrastructure.index.vector.backend.SentenceTransformer",
        return_value=mock_sentence_transformer,
    ):
        with patch(
            "osa.infrastructure.index.vector.backend.chromadb.PersistentClient",
            return_value=mock_client,
        ):
            backend = VectorStorageBackend("test-vector", config)

    return backend, mock_sentence_transformer, mock_collection


class TestVectorStorageBackendIngestBatch:
    """Tests for VectorStorageBackend.ingest_batch()."""

    @pytest.mark.asyncio
    async def test_ingest_batch_processes_all_records(self, vector_backend):
        """ingest_batch should process all records in the batch."""
        backend, mock_model, mock_collection = vector_backend

        # Configure mock to return embeddings for multiple texts
        mock_embeddings = MagicMock()
        mock_embeddings.tolist.return_value = [[0.1, 0.2], [0.3, 0.4], [0.5, 0.6]]
        mock_model.encode.return_value = mock_embeddings

        records = [
            ("urn:osa:test:rec:id1@1", {"title": "Record 1", "abstract": "Test 1"}),
            ("urn:osa:test:rec:id2@1", {"title": "Record 2", "abstract": "Test 2"}),
            ("urn:osa:test:rec:id3@1", {"title": "Record 3", "abstract": "Test 3"}),
        ]

        # Act
        await backend.ingest_batch(records)

        # Assert - model.encode was called once with all texts
        mock_model.encode.assert_called_once()
        texts_arg = mock_model.encode.call_args[0][0]
        assert len(texts_arg) == 3

        # Assert - collection.upsert was called once with all records
        mock_collection.upsert.assert_called_once()

    @pytest.mark.asyncio
    async def test_ingest_batch_empty_list_is_noop(self, vector_backend):
        """ingest_batch with empty list should not call any backend methods."""
        backend, mock_model, mock_collection = vector_backend

        # Act
        await backend.ingest_batch([])

        # Assert - no calls made
        mock_model.encode.assert_not_called()
        mock_collection.upsert.assert_not_called()

    @pytest.mark.asyncio
    async def test_ingest_batch_filters_non_chroma_types(self, vector_backend):
        """ingest_batch should filter out non-ChromaDB compatible metadata types."""
        backend, mock_model, mock_collection = vector_backend

        # Configure mock
        mock_embeddings = MagicMock()
        mock_embeddings.tolist.return_value = [[0.1, 0.2]]
        mock_model.encode.return_value = mock_embeddings

        records = [
            (
                "urn:osa:test:rec:id1@1",
                {
                    "title": "Test",  # str - keep
                    "count": 42,  # int - keep
                    "score": 0.95,  # float - keep
                    "active": True,  # bool - keep
                    "nested": {"key": "value"},  # dict - filter out
                    "items": [1, 2, 3],  # list - filter out
                },
            ),
        ]

        # Act
        await backend.ingest_batch(records)

        # Assert - upsert was called
        mock_collection.upsert.assert_called_once()
        call_kwargs = mock_collection.upsert.call_args
        metadatas = call_kwargs.kwargs.get("metadatas") or call_kwargs[1].get("metadatas")

        # Check filtered metadata
        assert len(metadatas) == 1
        meta = metadatas[0]
        assert "title" in meta
        assert "count" in meta
        assert "score" in meta
        assert "active" in meta
        assert "nested" not in meta
        assert "items" not in meta

    @pytest.mark.asyncio
    async def test_ingest_delegates_to_ingest_batch(self, vector_backend):
        """ingest() should delegate to ingest_batch() for single records."""
        backend, mock_model, mock_collection = vector_backend

        # Configure mock
        mock_embeddings = MagicMock()
        mock_embeddings.tolist.return_value = [[0.1, 0.2]]
        mock_model.encode.return_value = mock_embeddings

        # Act
        await backend.ingest("urn:osa:test:rec:id1@1", {"title": "Test"})

        # Assert - same behavior as ingest_batch with single record
        mock_model.encode.assert_called_once()
        mock_collection.upsert.assert_called_once()

    @pytest.mark.asyncio
    async def test_flush_is_noop(self, vector_backend):
        """flush() should be a no-op (backward compatibility)."""
        backend, mock_model, mock_collection = vector_backend

        # Act
        await backend.flush()

        # Assert - no calls made
        mock_model.encode.assert_not_called()
        mock_collection.upsert.assert_not_called()
