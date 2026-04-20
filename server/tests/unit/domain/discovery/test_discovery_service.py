"""Tests for DiscoveryService — FilterExpr validation, operator validation, delegation."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from osa.config import Config
from osa.domain.discovery.model.refs import MetadataFieldRef
from osa.domain.discovery.model.value import (
    And,
    ColumnInfo,
    FeatureCatalogEntry,
    FeatureRow,
    FilterOperator,
    Predicate,
    RecordSummary,
    SortOrder,
    decode_cursor,
    encode_cursor,
)
from osa.domain.discovery.service.discovery import DiscoveryService
from osa.domain.semantics.model.value import FieldType
from osa.domain.shared.error import ValidationError
from osa.domain.shared.model.srn import RecordSRN, SchemaSRN


SCHEMA_SRN = SchemaSRN.parse("urn:osa:localhost:schema:bio-sample@1.0.0")


def _config() -> Config:
    # Build a Config with minimal auth — tests don't hit JWT paths
    import os

    os.environ.setdefault("OSA_AUTH__JWT__SECRET", "a" * 64)  # Test-only secret
    os.environ.setdefault("OSA_BASE_URL", "http://localhost:8000")
    return Config()  # type: ignore[call-arg]


@pytest.fixture
def mock_read_store() -> AsyncMock:
    store = AsyncMock()
    store.search_records.return_value = []
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
    reader.get_fields_for_schema.return_value = {
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
    return DiscoveryService(
        read_store=mock_read_store,
        field_reader=mock_field_reader,
        config=_config(),
    )


def _eq(field: str, value: object) -> Predicate:
    return Predicate(field=MetadataFieldRef(field=field), op=FilterOperator.EQ, value=value)


class TestSearchRecordsValidation:
    async def test_rejects_unknown_filter_field(self, service: DiscoveryService) -> None:
        with pytest.raises(ValidationError, match="Unknown metadata field 'bogus'"):
            await service.search_records(
                filter_expr=_eq("bogus", "x"),
                schema_srn=SCHEMA_SRN,
                convention_srn=None,
                q=None,
                sort="published_at",
                order=SortOrder.DESC,
                cursor=None,
                limit=20,
            )

    async def test_rejects_invalid_operator_for_type(self, service: DiscoveryService) -> None:
        with pytest.raises(ValidationError, match="not valid"):
            await service.search_records(
                filter_expr=Predicate(
                    field=MetadataFieldRef(field="resolution"),
                    op=FilterOperator.CONTAINS,
                    value="x",
                ),
                schema_srn=SCHEMA_SRN,
                convention_srn=None,
                q=None,
                sort="published_at",
                order=SortOrder.DESC,
                cursor=None,
                limit=20,
            )

    async def test_rejects_unknown_sort_field(self, service: DiscoveryService) -> None:
        with pytest.raises(ValidationError, match="Unknown sort field"):
            await service.search_records(
                filter_expr=None,
                schema_srn=SCHEMA_SRN,
                convention_srn=None,
                q=None,
                sort="nonexistent",
                order=SortOrder.DESC,
                cursor=None,
                limit=20,
            )

    async def test_accepts_published_at_sort(self, service: DiscoveryService) -> None:
        result = await service.search_records(
            filter_expr=None,
            schema_srn=SCHEMA_SRN,
            convention_srn=None,
            q=None,
            sort="published_at",
            order=SortOrder.DESC,
            cursor=None,
            limit=20,
        )
        assert result.results == []

    async def test_accepts_metadata_field_sort(self, service: DiscoveryService) -> None:
        result = await service.search_records(
            filter_expr=None,
            schema_srn=SCHEMA_SRN,
            convention_srn=None,
            q=None,
            sort="resolution",
            order=SortOrder.ASC,
            cursor=None,
            limit=20,
        )
        assert result.results == []

    async def test_rejects_limit_too_low(self, service: DiscoveryService) -> None:
        with pytest.raises(ValidationError, match="limit"):
            await service.search_records(
                filter_expr=None,
                schema_srn=SCHEMA_SRN,
                convention_srn=None,
                q=None,
                sort="published_at",
                order=SortOrder.DESC,
                cursor=None,
                limit=0,
            )

    async def test_rejects_limit_too_high(self, service: DiscoveryService) -> None:
        with pytest.raises(ValidationError, match="limit"):
            await service.search_records(
                filter_expr=None,
                schema_srn=SCHEMA_SRN,
                convention_srn=None,
                q=None,
                sort="published_at",
                order=SortOrder.DESC,
                cursor=None,
                limit=101,
            )

    async def test_rejects_q_when_no_text_fields(self, mock_read_store: AsyncMock) -> None:
        no_text_reader = AsyncMock()
        no_text_reader.get_all_field_types.return_value = {"resolution": FieldType.NUMBER}
        no_text_reader.get_fields_for_schema.return_value = {"resolution": FieldType.NUMBER}
        svc = DiscoveryService(
            read_store=mock_read_store,
            field_reader=no_text_reader,
            config=_config(),
        )

        with pytest.raises(ValidationError, match="Free-text search is unavailable"):
            await svc.search_records(
                filter_expr=None,
                schema_srn=SCHEMA_SRN,
                convention_srn=None,
                q="kinase",
                sort="published_at",
                order=SortOrder.DESC,
                cursor=None,
                limit=20,
            )


class TestSearchRecordsDelegation:
    async def test_delegates_to_read_store(
        self, service: DiscoveryService, mock_read_store: AsyncMock
    ) -> None:
        await service.search_records(
            filter_expr=_eq("method", "X-ray"),
            schema_srn=SCHEMA_SRN,
            convention_srn=None,
            q=None,
            sort="published_at",
            order=SortOrder.DESC,
            cursor=None,
            limit=20,
        )

        mock_read_store.search_records.assert_called_once()
        call_kwargs = mock_read_store.search_records.call_args
        assert call_kwargs.kwargs["filter_expr"] is not None
        assert call_kwargs.kwargs["q"] is None
        assert call_kwargs.kwargs["sort"] == "published_at"
        assert call_kwargs.kwargs["limit"] == 21  # N+1

    async def test_decodes_cursor(
        self, service: DiscoveryService, mock_read_store: AsyncMock
    ) -> None:
        cursor = encode_cursor("2026-01-01", "urn:osa:localhost:rec:abc@1")
        await service.search_records(
            filter_expr=None,
            schema_srn=SCHEMA_SRN,
            convention_srn=None,
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
                filter_expr=None,
                schema_srn=SCHEMA_SRN,
                convention_srn=None,
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
        mock_read_store.search_records.return_value = [
            RecordSummary(srn=srn, published_at=ts, metadata={"title": f"r{i}"}) for i in range(2)
        ]

        result = await service.search_records(
            filter_expr=None,
            schema_srn=SCHEMA_SRN,
            convention_srn=None,
            q=None,
            sort="published_at",
            order=SortOrder.DESC,
            cursor=None,
            limit=1,
        )

        assert result.has_more is True
        assert result.cursor is not None
        assert len(result.results) == 1
        decoded = decode_cursor(result.cursor)
        assert decoded["id"] == str(srn)

    async def test_no_cursor_when_no_more_results(
        self, service: DiscoveryService, mock_read_store: AsyncMock
    ) -> None:
        mock_read_store.search_records.return_value = []

        result = await service.search_records(
            filter_expr=None,
            schema_srn=SCHEMA_SRN,
            convention_srn=None,
            q=None,
            sort="published_at",
            order=SortOrder.DESC,
            cursor=None,
            limit=20,
        )

        assert result.cursor is None
        assert result.has_more is False


class TestFilterBounds:
    async def test_depth_exceeded_raises(self, service: DiscoveryService) -> None:
        # Build a nest of AND that exceeds the default depth (10)
        leaf = _eq("title", "r")
        tree = leaf
        for _ in range(11):
            tree = And(operands=[tree, leaf])

        with pytest.raises(ValidationError, match="filter_depth_exceeded|depth"):
            await service.search_records(
                filter_expr=tree,
                schema_srn=SCHEMA_SRN,
                convention_srn=None,
                q=None,
                sort="published_at",
                order=SortOrder.DESC,
                cursor=None,
                limit=20,
            )


class TestFeatureCursorEncoding:
    async def test_cursor_encodes_row_id(
        self, mock_read_store: AsyncMock, mock_field_reader: AsyncMock
    ) -> None:
        srn = RecordSRN.parse("urn:osa:localhost:rec:abc@1")
        mock_read_store.get_feature_table_schema.return_value = FeatureCatalogEntry(
            hook_name="detect_pockets",
            columns=[ColumnInfo(name="score", type="number", required=True)],
            record_count=0,
        )
        mock_read_store.search_features.return_value = [
            FeatureRow(row_id=42, record_srn=srn, data={"score": 7.66}),
            FeatureRow(row_id=43, record_srn=srn, data={"score": 6.0}),
        ]

        service = DiscoveryService(
            read_store=mock_read_store,
            field_reader=mock_field_reader,
            config=_config(),
        )
        result = await service.search_features(
            hook_name="detect_pockets",
            filter_expr=None,
            schema_srn=None,
            record_srn=None,
            sort="score",
            order=SortOrder.DESC,
            cursor=None,
            limit=1,
        )

        assert result.has_more is True
        assert result.cursor is not None
        assert len(result.rows) == 1
        decoded = decode_cursor(result.cursor)
        assert decoded["id"] == 42
        assert decoded["s"] == 7.66
