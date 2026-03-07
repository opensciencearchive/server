"""Tests for feature enrichment — PostgresFeatureReader and RecordService integration."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from osa.domain.record.model.aggregate import Record
from osa.domain.record.query.get_record import GetRecord, GetRecordHandler, RecordDetail
from osa.domain.record.service.record import RecordService
from osa.domain.shared.model.srn import DepositionSRN, Domain, RecordSRN
from osa.infrastructure.persistence.adapter.feature_reader import PostgresFeatureReader


def _make_catalog_row(hook_name: str, pg_table: str, columns: list[dict] | None = None) -> dict:
    return {
        "hook_name": hook_name,
        "pg_table": pg_table,
        "feature_schema": {"columns": columns or []},
    }


class TestPostgresFeatureReader:
    @pytest.fixture
    def mock_session(self) -> AsyncMock:
        return AsyncMock()

    @pytest.fixture
    def reader(self, mock_session: AsyncMock) -> PostgresFeatureReader:
        return PostgresFeatureReader(session=mock_session)

    async def test_returns_dict_keyed_by_hook_name(
        self, reader: PostgresFeatureReader, mock_session: AsyncMock
    ) -> None:
        srn = RecordSRN.parse("urn:osa:localhost:rec:abc@1")

        # First call: catalog query
        catalog_result = MagicMock()
        catalog_result.mappings.return_value.all.return_value = [
            _make_catalog_row(
                "detect_pockets",
                "detect_pockets_v1",
                [
                    {"name": "score", "json_type": "number", "required": True},
                    {"name": "volume", "json_type": "number", "required": False},
                ],
            )
        ]

        # Second call: UNION ALL query returning {hook_name, row_data} mappings
        # row_data now excludes auto columns (jsonb_build_object only includes data cols)
        feature_result = MagicMock()
        feature_result.mappings.return_value = [
            {
                "hook_name": "detect_pockets",
                "row_data": {
                    "score": 7.66,
                    "volume": 1750.0,
                },
            }
        ]

        mock_session.execute.side_effect = [catalog_result, feature_result]

        result = await reader.get_features_for_record(srn)

        assert "detect_pockets" in result
        assert len(result["detect_pockets"]) == 1
        assert result["detect_pockets"][0]["score"] == 7.66
        assert result["detect_pockets"][0]["volume"] == 1750.0

    async def test_excludes_auto_columns(
        self, reader: PostgresFeatureReader, mock_session: AsyncMock
    ) -> None:
        srn = RecordSRN.parse("urn:osa:localhost:rec:abc@1")

        catalog_result = MagicMock()
        catalog_result.mappings.return_value.all.return_value = [
            _make_catalog_row(
                "test_hook",
                "test_hook_v1",
                [{"name": "metric", "json_type": "number", "required": True}],
            )
        ]

        feature_result = MagicMock()
        feature_result.mappings.return_value = [
            {
                "hook_name": "test_hook",
                "row_data": {
                    "metric": 3.14,
                },
            }
        ]

        mock_session.execute.side_effect = [catalog_result, feature_result]

        result = await reader.get_features_for_record(srn)

        row = result["test_hook"][0]
        assert "id" not in row
        assert "created_at" not in row
        assert "record_srn" not in row
        assert row["metric"] == 3.14

    async def test_returns_empty_when_no_feature_tables(
        self, reader: PostgresFeatureReader, mock_session: AsyncMock
    ) -> None:
        srn = RecordSRN.parse("urn:osa:localhost:rec:abc@1")

        catalog_result = MagicMock()
        catalog_result.mappings.return_value.all.return_value = []
        mock_session.execute.return_value = catalog_result

        result = await reader.get_features_for_record(srn)
        assert result == {}

    async def test_returns_empty_when_record_has_no_data(
        self, reader: PostgresFeatureReader, mock_session: AsyncMock
    ) -> None:
        srn = RecordSRN.parse("urn:osa:localhost:rec:abc@1")

        catalog_result = MagicMock()
        catalog_result.mappings.return_value.all.return_value = [
            _make_catalog_row(
                "detect_pockets",
                "detect_pockets_v1",
                [{"name": "score", "json_type": "number", "required": True}],
            )
        ]

        # UNION ALL returns no rows when record has no feature data
        feature_result = MagicMock()
        feature_result.mappings.return_value = []

        mock_session.execute.side_effect = [catalog_result, feature_result]

        result = await reader.get_features_for_record(srn)
        assert result == {}

    async def test_includes_data_from_multiple_tables(
        self, reader: PostgresFeatureReader, mock_session: AsyncMock
    ) -> None:
        srn = RecordSRN.parse("urn:osa:localhost:rec:abc@1")

        catalog_result = MagicMock()
        catalog_result.mappings.return_value.all.return_value = [
            _make_catalog_row(
                "hook_a", "hook_a_v1", [{"name": "x", "json_type": "integer", "required": True}]
            ),
            _make_catalog_row(
                "hook_b", "hook_b_v1", [{"name": "y", "json_type": "integer", "required": True}]
            ),
        ]

        # Single UNION ALL result containing rows from both tables
        # row_data now excludes auto columns
        feature_result = MagicMock()
        feature_result.mappings.return_value = [
            {
                "hook_name": "hook_a",
                "row_data": {"x": 1},
            },
            {
                "hook_name": "hook_b",
                "row_data": {"y": 2},
            },
        ]

        mock_session.execute.side_effect = [catalog_result, feature_result]

        result = await reader.get_features_for_record(srn)

        assert "hook_a" in result
        assert "hook_b" in result
        assert result["hook_a"][0]["x"] == 1
        assert result["hook_b"][0]["y"] == 2


def _make_record() -> Record:
    return Record(
        srn=RecordSRN.parse("urn:osa:localhost:rec:abc@1"),
        deposition_srn=DepositionSRN.parse("urn:osa:localhost:dep:dep1"),
        metadata={"title": "Test"},
        published_at=datetime.now(UTC),
    )


class TestRecordServiceFeatureEnrichment:
    async def test_get_features_delegates_to_reader(self) -> None:
        mock_repo = AsyncMock()
        mock_outbox = AsyncMock()
        mock_reader = AsyncMock()
        mock_reader.get_features_for_record.return_value = {"hook_a": [{"score": 1.0}]}

        service = RecordService(
            record_repo=mock_repo,
            outbox=mock_outbox,
            node_domain=Domain("localhost"),
            feature_reader=mock_reader,
        )

        srn = RecordSRN.parse("urn:osa:localhost:rec:abc@1")
        result = await service.get_features_for_record(srn)

        assert result == {"hook_a": [{"score": 1.0}]}
        mock_reader.get_features_for_record.assert_called_once_with(srn)


class TestGetRecordHandlerFeatureEnrichment:
    async def test_record_detail_includes_features(self) -> None:
        record = _make_record()
        mock_service = AsyncMock()
        mock_service.get.return_value = record
        mock_service.get_features_for_record.return_value = {"detect_pockets": [{"score": 7.66}]}

        handler = GetRecordHandler(record_service=mock_service)
        result: RecordDetail = await handler.run(GetRecord(srn=record.srn))

        assert result.features == {"detect_pockets": [{"score": 7.66}]}

    async def test_record_detail_features_empty_when_none(self) -> None:
        record = _make_record()
        mock_service = AsyncMock()
        mock_service.get.return_value = record
        mock_service.get_features_for_record.return_value = {}

        handler = GetRecordHandler(record_service=mock_service)
        result: RecordDetail = await handler.run(GetRecord(srn=record.srn))

        assert result.features == {}

    async def test_existing_behavior_preserved(self) -> None:
        record = _make_record()
        mock_service = AsyncMock()
        mock_service.get.return_value = record
        mock_service.get_features_for_record.return_value = {}

        handler = GetRecordHandler(record_service=mock_service)
        result: RecordDetail = await handler.run(GetRecord(srn=record.srn))

        assert result.srn == record.srn
        assert result.deposition_srn == record.deposition_srn
        assert result.metadata == record.metadata
        mock_service.get.assert_called_once_with(record.srn)
