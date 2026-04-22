"""Tests for SearchFeaturesHandler and DiscoveryService.search_features()."""

from unittest.mock import AsyncMock

import pytest

from osa.config import Config
from osa.domain.discovery.model.refs import FeatureFieldRef
from osa.domain.discovery.model.value import (
    ColumnInfo,
    FeatureCatalogEntry,
    FeatureRow,
    FeatureSearchResult,
    FilterOperator,
    Predicate,
    SortOrder,
)
from osa.domain.discovery.query.search_features import (
    SearchFeatures,
    SearchFeaturesHandler,
)
from osa.domain.discovery.service.discovery import DiscoveryService
from osa.domain.shared.error import NotFoundError, ValidationError
from osa.domain.shared.model.srn import RecordSRN


def _make_catalog_entry() -> FeatureCatalogEntry:
    return FeatureCatalogEntry(
        hook_name="detect_pockets",
        columns=[
            ColumnInfo(name="score", type="number", required=True),
            ColumnInfo(name="volume", type="number", required=True),
            ColumnInfo(name="label", type="string", required=False),
            ColumnInfo(name="is_active", type="boolean", required=False),
        ],
        record_count=10,
    )


def _config() -> Config:
    import os

    os.environ.setdefault("OSA_AUTH__JWT__SECRET", "a" * 64)
    os.environ.setdefault("OSA_BASE_URL", "http://localhost:8000")
    return Config()  # type: ignore[call-arg]


def _predicate(hook: str, column: str, op: FilterOperator, value: object) -> Predicate:
    return Predicate(field=FeatureFieldRef(hook=hook, column=column), op=op, value=value)


@pytest.fixture
def mock_read_store() -> AsyncMock:
    store = AsyncMock()
    store.get_feature_table_schema.return_value = _make_catalog_entry()
    store.search_features.return_value = []
    return store


@pytest.fixture
def mock_field_reader() -> AsyncMock:
    reader = AsyncMock()
    reader.get_all_field_types.return_value = {}
    reader.get_fields_for_schema.return_value = {}
    return reader


@pytest.fixture
def service(mock_read_store: AsyncMock, mock_field_reader: AsyncMock) -> DiscoveryService:
    return DiscoveryService(
        read_store=mock_read_store,
        field_reader=mock_field_reader,
        config=_config(),
    )


class TestSearchFeaturesHandler:
    async def test_public_auth_gate(self) -> None:
        from osa.domain.shared.authorization.gate import Public

        assert isinstance(SearchFeaturesHandler.__auth__, Public)

    async def test_delegates_to_service(self) -> None:
        mock_service = AsyncMock()
        mock_service.search_features.return_value = FeatureSearchResult(
            rows=[], cursor=None, has_more=False
        )

        handler = SearchFeaturesHandler(discovery_service=mock_service)
        await handler.run(SearchFeatures(hook_name="detect_pockets"))
        mock_service.search_features.assert_called_once()

    async def test_invalid_record_srn_raises_validation_error(self) -> None:
        mock_service = AsyncMock()
        handler = SearchFeaturesHandler(discovery_service=mock_service)

        with pytest.raises(ValidationError, match="not an OSA SRN") as exc_info:
            await handler.run(SearchFeatures(hook_name="detect_pockets", record_srn="not-a-srn"))

        assert exc_info.value.field == "record_srn"
        mock_service.search_features.assert_not_called()

    async def test_maps_rows_with_record_srn(self) -> None:
        srn = RecordSRN.parse("urn:osa:localhost:rec:abc@1")
        mock_service = AsyncMock()
        mock_service.search_features.return_value = FeatureSearchResult(
            rows=[FeatureRow(row_id=1, record_srn=srn, data={"score": 7.66})],
            cursor=None,
            has_more=False,
        )

        handler = SearchFeaturesHandler(discovery_service=mock_service)
        result = await handler.run(SearchFeatures(hook_name="detect_pockets"))

        assert result.rows[0]["record_srn"] == str(srn)
        assert result.rows[0]["score"] == 7.66


class TestDiscoveryServiceSearchFeatures:
    async def test_raises_not_found_for_unknown_hook(
        self, service: DiscoveryService, mock_read_store: AsyncMock
    ) -> None:
        mock_read_store.get_feature_table_schema.return_value = None

        with pytest.raises(NotFoundError):
            await service.search_features(
                hook_name="unknown_hook",
                filter_expr=None,
                schema_id=None,
                record_srn=None,
                sort="id",
                order=SortOrder.DESC,
                cursor=None,
                limit=50,
            )

    async def test_rejects_unknown_column(self, service: DiscoveryService) -> None:
        with pytest.raises(ValidationError, match="bogus"):
            await service.search_features(
                hook_name="detect_pockets",
                filter_expr=_predicate("detect_pockets", "bogus", FilterOperator.EQ, 1),
                schema_id=None,
                record_srn=None,
                sort="id",
                order=SortOrder.DESC,
                cursor=None,
                limit=50,
            )

    async def test_validates_operator_for_number_column(self, service: DiscoveryService) -> None:
        with pytest.raises(ValidationError, match="contains"):
            await service.search_features(
                hook_name="detect_pockets",
                filter_expr=_predicate("detect_pockets", "score", FilterOperator.CONTAINS, "x"),
                schema_id=None,
                record_srn=None,
                sort="id",
                order=SortOrder.DESC,
                cursor=None,
                limit=50,
            )

    async def test_accepts_string_contains_operator(self, service: DiscoveryService) -> None:
        await service.search_features(
            hook_name="detect_pockets",
            filter_expr=_predicate("detect_pockets", "label", FilterOperator.CONTAINS, "test"),
            schema_id=None,
            record_srn=None,
            sort="id",
            order=SortOrder.DESC,
            cursor=None,
            limit=50,
        )

    async def test_passes_record_srn_filter(
        self, service: DiscoveryService, mock_read_store: AsyncMock
    ) -> None:
        srn = RecordSRN.parse("urn:osa:localhost:rec:abc@1")
        await service.search_features(
            hook_name="detect_pockets",
            filter_expr=None,
            schema_id=None,
            record_srn=srn,
            sort="id",
            order=SortOrder.DESC,
            cursor=None,
            limit=50,
        )

        call_kwargs = mock_read_store.search_features.call_args
        assert call_kwargs.kwargs["record_srn"] == srn

    async def test_delegates_to_read_store(
        self, service: DiscoveryService, mock_read_store: AsyncMock
    ) -> None:
        await service.search_features(
            hook_name="detect_pockets",
            filter_expr=_predicate("detect_pockets", "score", FilterOperator.GTE, 6.0),
            schema_id=None,
            record_srn=None,
            sort="score",
            order=SortOrder.DESC,
            cursor=None,
            limit=50,
        )

        mock_read_store.search_features.assert_called_once()
        call_kwargs = mock_read_store.search_features.call_args
        assert call_kwargs.kwargs["hook_name"] == "detect_pockets"
        assert call_kwargs.kwargs["filter_expr"] is not None


class TestSearchFeaturesPagination:
    async def test_has_more_false_when_exactly_limit_rows(
        self, service: DiscoveryService, mock_read_store: AsyncMock
    ) -> None:
        srn = RecordSRN.parse("urn:osa:localhost:rec:abc@1")
        mock_read_store.search_features.return_value = [
            FeatureRow(row_id=i, record_srn=srn, data={"score": float(i)}) for i in range(3)
        ]

        result = await service.search_features(
            hook_name="detect_pockets",
            filter_expr=None,
            schema_id=None,
            record_srn=None,
            sort="score",
            order=SortOrder.DESC,
            cursor=None,
            limit=3,
        )

        assert result.has_more is False
        assert result.cursor is None
        assert len(result.rows) == 3

    async def test_has_more_true_when_more_than_limit_rows(
        self, service: DiscoveryService, mock_read_store: AsyncMock
    ) -> None:
        srn = RecordSRN.parse("urn:osa:localhost:rec:abc@1")
        mock_read_store.search_features.return_value = [
            FeatureRow(row_id=i, record_srn=srn, data={"score": float(i)}) for i in range(4)
        ]

        result = await service.search_features(
            hook_name="detect_pockets",
            filter_expr=None,
            schema_id=None,
            record_srn=None,
            sort="score",
            order=SortOrder.DESC,
            cursor=None,
            limit=3,
        )

        assert result.has_more is True
        assert result.cursor is not None
        assert len(result.rows) == 3

    async def test_passes_limit_plus_one_to_read_store(
        self, service: DiscoveryService, mock_read_store: AsyncMock
    ) -> None:
        await service.search_features(
            hook_name="detect_pockets",
            filter_expr=None,
            schema_id=None,
            record_srn=None,
            sort="score",
            order=SortOrder.DESC,
            cursor=None,
            limit=50,
        )

        call_kwargs = mock_read_store.search_features.call_args
        assert call_kwargs.kwargs["limit"] == 51
