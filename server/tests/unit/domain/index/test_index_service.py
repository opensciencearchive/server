"""Unit tests for IndexService."""

from unittest.mock import AsyncMock

import pytest

from osa.domain.index.model.registry import IndexRegistry
from osa.domain.index.service.index import IndexService


class FakeBackend:
    """Fake storage backend for testing."""

    def __init__(self, name: str, count: int = 0, healthy: bool = True):
        self._name = name
        self._count = count
        self._healthy = healthy
        self.count = AsyncMock(return_value=count)
        self.health = AsyncMock(return_value=healthy)

    @property
    def name(self) -> str:
        return self._name


class TestIndexService:
    """Tests for IndexService."""

    @pytest.mark.asyncio
    async def test_get_count_returns_backend_count(self):
        """get_count should return the backend's document count."""
        # Arrange
        backend = FakeBackend("vector", count=42)
        registry = IndexRegistry({"vector": backend})
        service = IndexService(indexes=registry)

        # Act
        result = await service.get_count("vector")

        # Assert
        assert result == 42
        backend.count.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_count_returns_none_for_unknown_backend(self):
        """get_count should return None for unknown backend."""
        # Arrange
        registry = IndexRegistry({})
        service = IndexService(indexes=registry)

        # Act
        result = await service.get_count("unknown")

        # Assert
        assert result is None

    @pytest.mark.asyncio
    async def test_check_health_returns_backend_health(self):
        """check_health should return the backend's health status."""
        # Arrange
        backend = FakeBackend("vector", healthy=True)
        registry = IndexRegistry({"vector": backend})
        service = IndexService(indexes=registry)

        # Act
        result = await service.check_health("vector")

        # Assert
        assert result is True
        backend.health.assert_called_once()

    @pytest.mark.asyncio
    async def test_check_health_returns_none_for_unknown_backend(self):
        """check_health should return None for unknown backend."""
        # Arrange
        registry = IndexRegistry({})
        service = IndexService(indexes=registry)

        # Act
        result = await service.check_health("unknown")

        # Assert
        assert result is None

    @pytest.mark.asyncio
    async def test_check_health_returns_false_for_unhealthy_backend(self):
        """check_health should return False for unhealthy backend."""
        # Arrange
        backend = FakeBackend("vector", healthy=False)
        registry = IndexRegistry({"vector": backend})
        service = IndexService(indexes=registry)

        # Act
        result = await service.check_health("vector")

        # Assert
        assert result is False
