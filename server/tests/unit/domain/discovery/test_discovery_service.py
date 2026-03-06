"""Tests for DiscoveryService — filter validation, operator validation, delegation."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from osa.domain.discovery.model.value import (
    ColumnInfo,
    FeatureCatalogEntry,
    FeatureRow,
    Filter,
    FilterOperator,
    RecordSummary,
    SortOrder,
)
from osa.domain.discovery.service.discovery import DiscoveryService
from osa.domain.semantics.model.value import FieldType
from osa.domain.shared.error import ValidationError
from osa.domain.shared.model.srn import RecordSRN


@pytest.fixture
def mock_read_store() -> AsyncMock:
    store = AsyncMock()
    store.search_records.return_value = ([], 0)
    return store


@pytest.fixture
def mock_field_reader() -> AsyncMock:
    reader = AsyncMock()
    reader.get_all_field_types.return_value = {
        "title": FieldType.TEXT,
        "resolution": FieldType.NUMBER,
        "method": FieldType.TERM,
        "published_date": FieldType.DATE,
        "is_public": FieldType.BOOLEAN,
        "homepage": FieldType.URL,
    }
    return reader


@pytest.fixture
def service(mock_read_store: AsyncMock, mock_field_reader: AsyncMock) -> DiscoveryService:
    return DiscoveryService(read_store=mock_read_store, field_reader=mock_field_reader)


class TestSearchRecordsValidation:
    async def test_rejects_unknown_filter_field(self, service: DiscoveryService) -> None:
        with pytest.raises(ValidationError, match="Unknown field 'bogus'"):
            await service.search_records(
                filters=[Filter(field="bogus", operator=FilterOperator.EQ, value="x")],
                q=None,
                sort="published_at",
                order=SortOrder.DESC,
                cursor=None,
                limit=20,
            )

    async def test_rejects_invalid_operator_for_type(self, service: DiscoveryService) -> None:
        with pytest.raises(ValidationError, match="contains"):
            await service.search_records(
                filters=[Filter(field="resolution", operator=FilterOperator.CONTAINS, value="x")],
                q=None,
                sort="published_at",
                order=SortOrder.DESC,
                cursor=None,
                limit=20,
            )

    async def test_rejects_unknown_sort_field(self, service: DiscoveryService) -> None:
        with pytest.raises(ValidationError, match="Unknown sort field"):
            await service.search_records(
                filters=[],
                q=None,
                sort="nonexistent",
                order=SortOrder.DESC,
                cursor=None,
                limit=20,
            )

    async def test_accepts_published_at_sort(self, service: DiscoveryService) -> None:
        result = await service.search_records(
            filters=[],
            q=None,
            sort="published_at",
            order=SortOrder.DESC,
            cursor=None,
            limit=20,
        )
        assert result.total == 0

    async def test_accepts_metadata_field_sort(self, service: DiscoveryService) -> None:
        result = await service.search_records(
            filters=[],
            q=None,
            sort="resolution",
            order=SortOrder.ASC,
            cursor=None,
            limit=20,
        )
        assert result.total == 0

    async def test_rejects_limit_too_low(self, service: DiscoveryService) -> None:
        with pytest.raises(ValidationError, match="limit"):
            await service.search_records(
                filters=[],
                q=None,
                sort="published_at",
                order=SortOrder.DESC,
                cursor=None,
                limit=0,
            )

    async def test_rejects_limit_too_high(self, service: DiscoveryService) -> None:
        with pytest.raises(ValidationError, match="limit"):
            await service.search_records(
                filters=[],
                q=None,
                sort="published_at",
                order=SortOrder.DESC,
                cursor=None,
                limit=101,
            )


class TestSearchRecordsDelegation:
    async def test_delegates_to_read_store(
        self, service: DiscoveryService, mock_read_store: AsyncMock
    ) -> None:
        await service.search_records(
            filters=[Filter(field="method", operator=FilterOperator.EQ, value="X-ray")],
            q=None,
            sort="published_at",
            order=SortOrder.DESC,
            cursor=None,
            limit=20,
        )

        mock_read_store.search_records.assert_called_once()
        call_kwargs = mock_read_store.search_records.call_args
        assert len(call_kwargs.kwargs["filters"]) == 1
        assert call_kwargs.kwargs["q"] is None
        assert call_kwargs.kwargs["sort"] == "published_at"
        assert call_kwargs.kwargs["limit"] == 20

    async def test_extracts_text_fields_for_q(
        self, service: DiscoveryService, mock_read_store: AsyncMock
    ) -> None:
        await service.search_records(
            filters=[],
            q="kinase",
            sort="published_at",
            order=SortOrder.DESC,
            cursor=None,
            limit=20,
        )

        call_kwargs = mock_read_store.search_records.call_args
        text_fields = call_kwargs.kwargs["text_fields"]
        # title (TEXT) and homepage (URL) are text-searchable
        assert "title" in text_fields
        assert "homepage" in text_fields
        assert "resolution" not in text_fields

    async def test_decodes_cursor(
        self, service: DiscoveryService, mock_read_store: AsyncMock
    ) -> None:
        from osa.domain.discovery.model.value import encode_cursor

        cursor = encode_cursor("2026-01-01", "urn:osa:localhost:rec:abc@1")
        await service.search_records(
            filters=[],
            q=None,
            sort="published_at",
            order=SortOrder.DESC,
            cursor=cursor,
            limit=20,
        )

        call_kwargs = mock_read_store.search_records.call_args
        decoded = call_kwargs.kwargs["cursor"]
        assert decoded["s"] == "2026-01-01"
        assert decoded["id"] == "urn:osa:localhost:rec:abc@1"

    async def test_invalid_cursor_raises(self, service: DiscoveryService) -> None:
        with pytest.raises(ValidationError, match="cursor"):
            await service.search_records(
                filters=[],
                q=None,
                sort="published_at",
                order=SortOrder.DESC,
                cursor="not-a-cursor!!!",
                limit=20,
            )

    async def test_encodes_next_cursor_from_results(
        self, service: DiscoveryService, mock_read_store: AsyncMock
    ) -> None:
        srn = RecordSRN.parse("urn:osa:localhost:rec:abc@1")
        ts = datetime(2026, 1, 1, tzinfo=UTC)
        mock_read_store.search_records.return_value = (
            [RecordSummary(srn=srn, published_at=ts, metadata={"title": "Test"})],
            5,
        )

        result = await service.search_records(
            filters=[],
            q=None,
            sort="published_at",
            order=SortOrder.DESC,
            cursor=None,
            limit=1,
        )

        assert result.has_more is True
        assert result.cursor is not None

        from osa.domain.discovery.model.value import decode_cursor

        decoded = decode_cursor(result.cursor)
        assert decoded["id"] == str(srn)

    async def test_no_cursor_when_no_more_results(
        self, service: DiscoveryService, mock_read_store: AsyncMock
    ) -> None:
        mock_read_store.search_records.return_value = ([], 0)

        result = await service.search_records(
            filters=[],
            q=None,
            sort="published_at",
            order=SortOrder.DESC,
            cursor=None,
            limit=20,
        )

        assert result.cursor is None
        assert result.has_more is False


class TestSearchRecordsFieldTypes:
    async def test_passes_field_types_to_read_store(
        self, service: DiscoveryService, mock_read_store: AsyncMock
    ) -> None:
        await service.search_records(
            filters=[],
            q=None,
            sort="published_at",
            order=SortOrder.DESC,
            cursor=None,
            limit=20,
        )

        call_kwargs = mock_read_store.search_records.call_args
        field_types = call_kwargs.kwargs["field_types"]
        assert field_types["resolution"] == FieldType.NUMBER
        assert field_types["title"] == FieldType.TEXT


class TestFeatureCursorEncoding:
    async def test_cursor_encodes_row_id(
        self, mock_read_store: AsyncMock, mock_field_reader: AsyncMock
    ) -> None:
        from osa.domain.discovery.model.value import decode_cursor

        srn = RecordSRN.parse("urn:osa:localhost:rec:abc@1")
        mock_read_store.get_feature_table_schema.return_value = FeatureCatalogEntry(
            hook_name="detect_pockets",
            columns=[ColumnInfo(name="score", type="number", required=True)],
            record_count=0,
        )
        mock_read_store.search_features.return_value = (
            [FeatureRow(row_id=42, record_srn=srn, data={"score": 7.66})],
            5,
        )

        service = DiscoveryService(read_store=mock_read_store, field_reader=mock_field_reader)
        result = await service.search_features(
            hook_name="detect_pockets",
            filters=[],
            record_srn=None,
            sort="score",
            order=SortOrder.DESC,
            cursor=None,
            limit=1,
        )

        assert result.has_more is True
        assert result.cursor is not None
        decoded = decode_cursor(result.cursor)
        assert decoded["id"] == 42
        assert decoded["s"] == 7.66

    async def test_cursor_uses_row_id_for_id_sort(
        self, mock_read_store: AsyncMock, mock_field_reader: AsyncMock
    ) -> None:
        from osa.domain.discovery.model.value import decode_cursor

        srn = RecordSRN.parse("urn:osa:localhost:rec:abc@1")
        mock_read_store.get_feature_table_schema.return_value = FeatureCatalogEntry(
            hook_name="detect_pockets",
            columns=[ColumnInfo(name="score", type="number", required=True)],
            record_count=0,
        )
        mock_read_store.search_features.return_value = (
            [FeatureRow(row_id=99, record_srn=srn, data={"score": 5.0})],
            3,
        )

        service = DiscoveryService(read_store=mock_read_store, field_reader=mock_field_reader)
        result = await service.search_features(
            hook_name="detect_pockets",
            filters=[],
            record_srn=None,
            sort="id",
            order=SortOrder.DESC,
            cursor=None,
            limit=1,
        )

        assert result.has_more is True
        assert result.cursor is not None
        decoded = decode_cursor(result.cursor)
        # When sort is "id", sort_val should be the row_id itself
        assert decoded["s"] == 99
        assert decoded["id"] == 99
