"""Tests for SearchRecordsHandler — auth gate, delegation, result mapping."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from osa.domain.discovery.model.value import RecordSearchResult, RecordSummary, SortOrder
from osa.domain.discovery.query.search_records import (
    SearchRecords,
    SearchRecordsHandler,
    SearchRecordsResult,
)
from osa.domain.shared.model.srn import RecordSRN


@pytest.fixture
def mock_service() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def handler(mock_service: AsyncMock) -> SearchRecordsHandler:
    return SearchRecordsHandler(discovery_service=mock_service)


class TestSearchRecordsHandler:
    async def test_public_auth_gate(self, handler: SearchRecordsHandler) -> None:
        from osa.domain.shared.authorization.gate import Public

        assert isinstance(handler.__auth__, Public)

    async def test_delegates_to_service(
        self, handler: SearchRecordsHandler, mock_service: AsyncMock
    ) -> None:
        mock_service.search_records.return_value = RecordSearchResult(
            results=[], cursor=None, has_more=False
        )
        cmd = SearchRecords()
        await handler.run(cmd)

        mock_service.search_records.assert_called_once_with(
            filters=[],
            q=None,
            sort="published_at",
            order=SortOrder.DESC,
            cursor=None,
            limit=20,
        )

    async def test_maps_results(
        self, handler: SearchRecordsHandler, mock_service: AsyncMock
    ) -> None:
        srn = RecordSRN.parse("urn:osa:localhost:rec:abc@1")
        ts = datetime(2026, 1, 1, tzinfo=UTC)
        mock_service.search_records.return_value = RecordSearchResult(
            results=[RecordSummary(srn=srn, published_at=ts, metadata={"title": "Test"})],
            cursor="abc123",
            has_more=False,
        )

        result: SearchRecordsResult = await handler.run(SearchRecords())

        assert result.cursor == "abc123"
        assert result.has_more is False
        assert result.results[0]["srn"] == str(srn)
        assert result.results[0]["metadata"] == {"title": "Test"}
