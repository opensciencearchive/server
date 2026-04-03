"""Unit tests for decoupled ValidationService.

Tests for User Story 3: Cross-domain decoupling.
Verifies ValidationService uses event payload data (hooks, files_dir)
instead of querying DepositionRepository/ConventionRepository.
"""

import inspect
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from osa.domain.shared.model.hook import (
    ColumnDef,
    HookDefinition,
    OciConfig,
    TableFeatureSpec,
)
from osa.domain.shared.model.srn import ConventionSRN, DepositionSRN, Domain
from osa.domain.validation.model import RunStatus
from osa.domain.validation.model.hook_result import HookResult, HookStatus
from osa.domain.validation.service.validation import ValidationService


def _make_dep_srn() -> DepositionSRN:
    return DepositionSRN.parse("urn:osa:localhost:dep:test-dep")


def _make_conv_srn() -> ConventionSRN:
    return ConventionSRN.parse("urn:osa:localhost:conv:test@1.0.0")


def _make_hook_definition() -> HookDefinition:
    return HookDefinition(
        name="pocketeer",
        runtime=OciConfig(
            image="osa-hooks/pocketeer:latest",
            digest="sha256:abc123",
            config={"threshold": 0.5},
        ),
        feature=TableFeatureSpec(
            cardinality="many",
            columns=[ColumnDef(name="score", json_type="number", required=True)],
        ),
    )


class TestDecoupledValidationService:
    def test_no_deposition_repo_dependency(self):
        """ValidationService no longer depends on DepositionRepository."""
        sig = inspect.signature(ValidationService.__init__)
        param_names = list(sig.parameters.keys())
        assert "deposition_repo" not in param_names

    def test_no_convention_repo_dependency(self):
        """ValidationService no longer depends on ConventionRepository."""
        sig = inspect.signature(ValidationService.__init__)
        param_names = list(sig.parameters.keys())
        assert "convention_repo" not in param_names

    def test_no_file_storage_dependency(self):
        """ValidationService no longer depends on FileStoragePort."""
        sig = inspect.signature(ValidationService.__init__)
        param_names = list(sig.parameters.keys())
        assert "file_storage" not in param_names

    @pytest.mark.asyncio
    async def test_validate_deposition_uses_event_data(self):
        """validate_deposition accepts hooks/metadata directly."""
        run_repo = AsyncMock()
        run_repo.save = AsyncMock()
        hook_runner = AsyncMock()
        hook_runner.run.return_value = HookResult(
            hook_name="pocketeer",
            status=HookStatus.PASSED,
            duration_seconds=1.0,
        )
        hook_storage = MagicMock()
        hook_storage.get_hook_output_dir.return_value = Path("/tmp/hooks/pocketeer")
        hook_storage.get_files_dir.return_value = Path("/data/files/test-dep")
        hook_storage.write_checkpoint = AsyncMock()
        hook_storage.write_batch_outcomes = AsyncMock()

        service = ValidationService(
            run_repo=run_repo,
            hook_runner=hook_runner,
            hook_storage=hook_storage,
            node_domain=Domain("localhost"),
        )

        hook = _make_hook_definition()
        run, hook_results = await service.validate_deposition(
            deposition_srn=_make_dep_srn(),
            convention_srn=_make_conv_srn(),
            metadata={"pdb_id": "4HHB"},
            hooks=[hook],
        )

        assert run.status == RunStatus.COMPLETED
        assert len(hook_results) == 1
        assert hook_results[0].status == HookStatus.PASSED
