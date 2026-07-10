"""Unit tests for FeatureService.insert_features_for_record.

Formerly co-located with the ``InsertRecordFeatures`` event handler; that handler
was deleted when the pipeline collapsed into orchestrated workflows (#160). The
service method it delegated to lives on — ProcessSubmission calls it directly in
its INSERT_FEATURES stage.
"""

from unittest.mock import AsyncMock

import pytest

from osa.domain.feature.service.feature import FeatureService
from osa.domain.shared.model.hook import FeatureName
from osa.domain.shared.model.provenance import RunRef
from osa.domain.shared.model.srn import RecordSRN


def _make_record_srn() -> RecordSRN:
    return RecordSRN.parse("urn:osa:localhost:rec:test-rec@1")


def _make_run_ref() -> RunRef:
    return RunRef(run_id="run-abc", release_id="rel-xyz")


def _make_feature_service(
    feature_store: AsyncMock | None = None,
    feature_storage: AsyncMock | None = None,
) -> FeatureService:
    return FeatureService(
        feature_store=feature_store or AsyncMock(),
        feature_storage=feature_storage or AsyncMock(),
    )


class TestFeatureServiceInsertFeaturesForRecord:
    @pytest.mark.asyncio
    async def test_inserts_features_from_cold_storage(self):
        """Reads features.json from cold storage and inserts with record_srn."""
        feature_storage = AsyncMock()
        feature_storage.hook_features_exist.return_value = True
        feature_storage.read_run_ref.return_value = _make_run_ref()
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
            expected_features=[FeatureName("pocket_detect")],
        )

        # run_id (from the hook output dir's run.json) is stamped on every row (#145).
        feature_store.insert_features.assert_called_once_with(
            "pocket_detect",
            str(_make_record_srn()),
            [{"score": 0.95}, {"score": 0.82}],
            "run-abc",
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
            expected_features=[FeatureName("pocket_detect")],
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
            expected_features=[FeatureName("pocket_detect")],
        )

        feature_store.insert_features.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_features_without_run_json(self):
        """A hook with features but no run.json is skipped (no provenance, #145)."""
        feature_storage = AsyncMock()
        feature_storage.hook_features_exist.return_value = True
        feature_storage.read_run_ref.return_value = None

        feature_store = AsyncMock()

        service = _make_feature_service(
            feature_store=feature_store,
            feature_storage=feature_storage,
        )

        await service.insert_features_for_record(
            hook_output_dir="/fake/output/dir",
            record_srn=str(_make_record_srn()),
            expected_features=[FeatureName("pocket_detect")],
        )

        feature_storage.read_hook_features.assert_not_called()
        feature_store.insert_features.assert_not_called()

    @pytest.mark.asyncio
    async def test_handles_multiple_features(self):
        """Processes all expected features."""
        feature_storage = AsyncMock()
        feature_storage.hook_features_exist.return_value = True
        feature_storage.read_run_ref.return_value = _make_run_ref()
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
            expected_features=[FeatureName("hook_a"), FeatureName("hook_b")],
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
