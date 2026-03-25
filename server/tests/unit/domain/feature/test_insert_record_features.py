"""Unit tests for InsertRecordFeatures event handler and FeatureService.insert_features_for_record."""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from osa.domain.feature.handler.insert_record_features import InsertRecordFeatures
from osa.domain.feature.service.feature import FeatureService
from osa.domain.record.event.record_published import RecordPublished
from osa.domain.shared.event import EventId
from osa.domain.shared.model.source import DepositionSource, HarvestSource
from osa.domain.shared.model.srn import (
    ConventionSRN,
    RecordSRN,
)


def _make_record_srn() -> RecordSRN:
    return RecordSRN.parse("urn:osa:localhost:rec:test-rec@1")


def _make_conv_srn() -> ConventionSRN:
    return ConventionSRN.parse("urn:osa:localhost:conv:test@1.0.0")


def _make_event(
    expected_features: list[str] | None = None,
) -> RecordPublished:
    return RecordPublished(
        id=EventId(uuid4()),
        record_srn=_make_record_srn(),
        source=DepositionSource(id="urn:osa:localhost:dep:test-dep"),
        metadata={"title": "Test"},
        convention_srn=_make_conv_srn(),
        expected_features=expected_features or [],
    )


def _make_feature_service(
    feature_store: AsyncMock | None = None,
    feature_storage: AsyncMock | None = None,
) -> FeatureService:
    return FeatureService(
        feature_store=feature_store or AsyncMock(),
        feature_storage=feature_storage or AsyncMock(),
    )


def _make_handler(
    feature_service: FeatureService | AsyncMock | None = None,
    feature_storage: MagicMock | None = None,
) -> InsertRecordFeatures:
    storage = feature_storage or MagicMock()
    if not feature_storage:
        storage.get_hook_output_root = MagicMock(return_value="/fake/output/dir")
    return InsertRecordFeatures(
        feature_service=feature_service or AsyncMock(),
        feature_storage=storage,
    )


class TestInsertRecordFeaturesHandler:
    @pytest.mark.asyncio
    async def test_delegates_to_feature_service(self):
        """Handler delegates to FeatureService.insert_features_for_record."""
        feature_service = AsyncMock()
        handler = _make_handler(feature_service=feature_service)

        event = _make_event(
            expected_features=["pocket_detect"],
        )
        await handler.handle(event)

        feature_service.insert_features_for_record.assert_called_once_with(
            hook_output_dir="/fake/output/dir",
            record_srn=str(event.record_srn),
            expected_features=["pocket_detect"],
        )


class TestFeatureServiceInsertFeaturesForRecord:
    @pytest.mark.asyncio
    async def test_inserts_features_from_cold_storage(self):
        """Reads features.json from cold storage and inserts with record_srn."""
        feature_storage = AsyncMock()
        feature_storage.hook_features_exist.return_value = True
        feature_storage.read_hook_features.return_value = [{"score": 0.95}, {"score": 0.82}]

        feature_store = AsyncMock()
        feature_store.insert_features.return_value = 2

        service = _make_feature_service(
            feature_store=feature_store,
            feature_storage=feature_storage,
        )

        await service.insert_features_for_record(
            hook_output_dir="/fake/output/dir",
            record_srn=str(_make_record_srn()),
            expected_features=["pocket_detect"],
        )

        feature_store.insert_features.assert_called_once_with(
            "pocket_detect",
            str(_make_record_srn()),
            [{"score": 0.95}, {"score": 0.82}],
        )

    @pytest.mark.asyncio
    async def test_skips_features_without_features_file(self):
        """Features that didn't produce features.json are skipped with a warning."""
        feature_storage = AsyncMock()
        feature_storage.hook_features_exist.return_value = False

        feature_store = AsyncMock()

        service = _make_feature_service(
            feature_store=feature_store,
            feature_storage=feature_storage,
        )

        await service.insert_features_for_record(
            hook_output_dir="/fake/output/dir",
            record_srn=str(_make_record_srn()),
            expected_features=["pocket_detect"],
        )

        feature_storage.read_hook_features.assert_not_called()
        feature_store.insert_features.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_empty_feature_list(self):
        """Features that produced empty features.json are skipped."""
        feature_storage = AsyncMock()
        feature_storage.hook_features_exist.return_value = True
        feature_storage.read_hook_features.return_value = []

        feature_store = AsyncMock()

        service = _make_feature_service(
            feature_store=feature_store,
            feature_storage=feature_storage,
        )

        await service.insert_features_for_record(
            hook_output_dir="/fake/output/dir",
            record_srn=str(_make_record_srn()),
            expected_features=["pocket_detect"],
        )

        feature_store.insert_features.assert_not_called()

    @pytest.mark.asyncio
    async def test_handles_multiple_features(self):
        """Processes all expected features."""
        feature_storage = AsyncMock()
        feature_storage.hook_features_exist.return_value = True
        feature_storage.read_hook_features.side_effect = [
            [{"score": 0.9}],
            [{"score": 0.8}],
        ]

        feature_store = AsyncMock()
        feature_store.insert_features.return_value = 1

        service = _make_feature_service(
            feature_store=feature_store,
            feature_storage=feature_storage,
        )

        await service.insert_features_for_record(
            hook_output_dir="/fake/output/dir",
            record_srn=str(_make_record_srn()),
            expected_features=["hook_a", "hook_b"],
        )

        assert feature_store.insert_features.call_count == 2

    @pytest.mark.asyncio
    async def test_no_features_is_noop(self):
        """No-op when expected_features list is empty."""
        feature_store = AsyncMock()
        feature_storage = AsyncMock()

        service = _make_feature_service(
            feature_store=feature_store,
            feature_storage=feature_storage,
        )

        await service.insert_features_for_record(
            hook_output_dir="/fake/output/dir",
            record_srn=str(_make_record_srn()),
            expected_features=[],
        )

        feature_storage.hook_features_exist.assert_not_called()
        feature_store.insert_features.assert_not_called()


class TestInsertRecordFeaturesHarvestSource:
    """US2: InsertRecordFeatures works identically for harvest-sourced records."""

    @pytest.mark.asyncio
    async def test_harvest_source_uses_source_fields(self):
        """Handler uses source type and id from event regardless of source type."""
        feature_service = AsyncMock()
        storage = MagicMock()
        storage.get_hook_output_root.return_value = "/fake/harvest/dir"
        handler = _make_handler(feature_service=feature_service, feature_storage=storage)

        event = RecordPublished(
            id=EventId(uuid4()),
            record_srn=_make_record_srn(),
            source=HarvestSource(
                id="run-123-pdb-456",
                harvest_run_srn="urn:osa:localhost:val:run123",
                upstream_source="pdb",
            ),
            metadata={"title": "Harvested"},
            convention_srn=_make_conv_srn(),
            expected_features=["pocket_detect"],
        )
        await handler.handle(event)

        storage.get_hook_output_root.assert_called_once_with("harvest", "run-123-pdb-456")
        feature_service.insert_features_for_record.assert_called_once_with(
            hook_output_dir="/fake/harvest/dir",
            record_srn=str(_make_record_srn()),
            expected_features=["pocket_detect"],
        )
