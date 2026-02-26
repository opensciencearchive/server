"""Unit tests for decoupled FeatureService.

Tests for User Story 3: Cross-domain decoupling.
Verifies FeatureService uses event payload data instead of querying repos.
"""

import inspect
from unittest.mock import AsyncMock

import pytest

from osa.domain.shared.model.hook import ColumnDef
from osa.domain.shared.model.hook_snapshot import HookSnapshot
from osa.domain.shared.model.srn import DepositionSRN
from osa.domain.feature.service.feature import FeatureService


def _make_dep_srn() -> DepositionSRN:
    return DepositionSRN.parse("urn:osa:localhost:dep:test-dep")


def _make_hook_snapshot() -> HookSnapshot:
    return HookSnapshot(
        name="pocketeer",
        image="osa-hooks/pocketeer:latest",
        digest="sha256:abc123",
        features=[ColumnDef(name="score", json_type="number", required=True)],
    )


class TestDecoupledFeatureService:
    def test_no_deposition_repo_dependency(self):
        """FeatureService no longer depends on DepositionRepository."""
        sig = inspect.signature(FeatureService.__init__)
        param_names = list(sig.parameters.keys())
        assert "deposition_repo" not in param_names

    def test_no_convention_repo_dependency(self):
        """FeatureService no longer depends on ConventionRepository."""
        sig = inspect.signature(FeatureService.__init__)
        param_names = list(sig.parameters.keys())
        assert "convention_repo" not in param_names

    def test_no_file_storage_dependency(self):
        """FeatureService no longer depends on FileStoragePort."""
        sig = inspect.signature(FeatureService.__init__)
        param_names = list(sig.parameters.keys())
        assert "file_storage" not in param_names

    @pytest.mark.asyncio
    async def test_insert_features_for_record_uses_event_data(self):
        """insert_features_for_record accepts hooks directly."""
        feature_store = AsyncMock()
        feature_store.insert_features.return_value = 3
        feature_storage = AsyncMock()
        feature_storage.hook_features_exist.return_value = True
        feature_storage.read_hook_features.return_value = [
            {"score": 0.8},
            {"score": 0.6},
            {"score": 0.4},
        ]

        service = FeatureService(
            feature_store=feature_store,
            feature_storage=feature_storage,
        )

        hook = _make_hook_snapshot()
        await service.insert_features_for_record(
            deposition_srn=_make_dep_srn(),
            record_srn="urn:osa:localhost:rec:test@1",
            hooks=[hook],
        )

        feature_storage.hook_features_exist.assert_called_once_with(_make_dep_srn(), "pocketeer")
        feature_store.insert_features.assert_called_once()
