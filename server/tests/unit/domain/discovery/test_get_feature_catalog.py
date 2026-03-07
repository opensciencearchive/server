"""Tests for GetFeatureCatalogHandler and DiscoveryService.get_feature_catalog()."""

from unittest.mock import AsyncMock

import pytest

from osa.domain.discovery.model.value import ColumnInfo, FeatureCatalogEntry
from osa.domain.discovery.query.get_feature_catalog import (
    GetFeatureCatalog,
    GetFeatureCatalogHandler,
    GetFeatureCatalogResult,
)
from osa.domain.discovery.service.discovery import DiscoveryService


@pytest.fixture
def mock_read_store() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def mock_field_reader() -> AsyncMock:
    reader = AsyncMock()
    reader.get_all_field_types.return_value = {}
    return reader


@pytest.fixture
def service(mock_read_store: AsyncMock, mock_field_reader: AsyncMock) -> DiscoveryService:
    return DiscoveryService(read_store=mock_read_store, field_reader=mock_field_reader)


class TestGetFeatureCatalogHandler:
    async def test_public_auth_gate(self) -> None:
        from osa.domain.shared.authorization.gate import Public

        assert isinstance(GetFeatureCatalogHandler.__auth__, Public)

    async def test_delegates_to_service(self) -> None:
        mock_service = AsyncMock()
        from osa.domain.discovery.model.value import FeatureCatalog

        mock_service.get_feature_catalog.return_value = FeatureCatalog(tables=[])

        handler = GetFeatureCatalogHandler(discovery_service=mock_service)
        result: GetFeatureCatalogResult = await handler.run(GetFeatureCatalog())

        assert result.tables == []
        mock_service.get_feature_catalog.assert_called_once()

    async def test_returns_correct_structure(self) -> None:
        mock_service = AsyncMock()
        from osa.domain.discovery.model.value import FeatureCatalog

        mock_service.get_feature_catalog.return_value = FeatureCatalog(
            tables=[
                FeatureCatalogEntry(
                    hook_name="detect_pockets",
                    columns=[
                        ColumnInfo(name="score", type="number", required=True),
                        ColumnInfo(name="volume", type="number", required=True),
                    ],
                    record_count=142,
                )
            ]
        )

        handler = GetFeatureCatalogHandler(discovery_service=mock_service)
        result: GetFeatureCatalogResult = await handler.run(GetFeatureCatalog())

        assert len(result.tables) == 1
        assert result.tables[0]["hook_name"] == "detect_pockets"
        assert result.tables[0]["record_count"] == 142
        assert len(result.tables[0]["columns"]) == 2


class TestDiscoveryServiceGetFeatureCatalog:
    async def test_delegates_to_read_store(
        self, service: DiscoveryService, mock_read_store: AsyncMock
    ) -> None:
        mock_read_store.get_feature_catalog.return_value = []
        result = await service.get_feature_catalog()
        assert result.tables == []
        mock_read_store.get_feature_catalog.assert_called_once()

    async def test_returns_entries_from_store(
        self, service: DiscoveryService, mock_read_store: AsyncMock
    ) -> None:
        entries = [
            FeatureCatalogEntry(
                hook_name="test_hook",
                columns=[ColumnInfo(name="x", type="number", required=True)],
                record_count=10,
            )
        ]
        mock_read_store.get_feature_catalog.return_value = entries

        result = await service.get_feature_catalog()
        assert len(result.tables) == 1
        assert result.tables[0].hook_name == "test_hook"
