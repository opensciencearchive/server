from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from osa.domain.shared.model.srn import (
    Domain,
    LocalId,
    Semver,
    TraitSRN,
)
from osa.domain.validation.model import (
    CheckStatus,
    RunStatus,
    Trait,
    TraitStatus,
    Validator,
    ValidatorLimits,
    ValidatorRef,
)
from osa.domain.validation.port.runner import ValidationInputs, ValidatorOutput
from osa.domain.validation.service import ValidationService


def make_trait(slug: str, image: str = "test/validator") -> Trait:
    """Helper to create a test trait."""
    return Trait(
        srn=TraitSRN(
            domain=Domain("osap.org"),
            id=LocalId(slug),
            version=Semver("1.0.0"),
        ),
        slug=slug,
        name=f"Test Trait: {slug}",
        description=f"Test trait for {slug}",
        validator=Validator(
            ref=ValidatorRef(image=image, digest="sha256:abc123"),
            limits=ValidatorLimits(timeout_seconds=30),
        ),
        status=TraitStatus.ACTIVE,
        created_at=datetime.now(timezone.utc),
    )


class TestValidationService:
    @pytest.fixture
    def mock_trait_repo(self):
        repo = AsyncMock()
        return repo

    @pytest.fixture
    def mock_run_repo(self):
        repo = AsyncMock()
        return repo

    @pytest.fixture
    def mock_runner(self):
        runner = AsyncMock()
        return runner

    @pytest.fixture
    def service(self, mock_trait_repo, mock_run_repo, mock_runner):
        return ValidationService(
            trait_repo=mock_trait_repo,
            run_repo=mock_run_repo,
            runner=mock_runner,
            node_domain=Domain("localhost"),
        )

    @pytest.fixture
    def validation_inputs(self):
        return ValidationInputs(
            record_json={"title": "Test Dataset", "temperature": 293.15},
        )

    async def test_validate_all_pass(
        self, service, mock_trait_repo, mock_runner, validation_inputs
    ):
        """Test validation where all traits pass."""
        trait = make_trait("si-units")
        mock_trait_repo.get_or_fetch.return_value = trait
        mock_runner.run.return_value = ValidatorOutput(
            status=CheckStatus.PASSED,
            checks=[{"id": "check-1", "status": "passed"}],
        )

        trait_srns = [trait.srn]
        run = await service.validate(trait_srns, validation_inputs)

        assert run.status == RunStatus.COMPLETED
        assert len(run.results) == 1
        assert run.results[0].status == CheckStatus.PASSED
        assert run.started_at is not None
        assert run.completed_at is not None

    async def test_validate_with_failure(
        self, service, mock_trait_repo, mock_runner, validation_inputs
    ):
        """Test validation where a trait fails."""
        trait = make_trait("si-units")
        mock_trait_repo.get_or_fetch.return_value = trait
        mock_runner.run.return_value = ValidatorOutput(
            status=CheckStatus.FAILED,
            checks=[{"id": "check-1", "status": "failed", "message": "Invalid units"}],
        )

        trait_srns = [trait.srn]
        run = await service.validate(trait_srns, validation_inputs)

        assert run.status == RunStatus.FAILED
        assert len(run.results) == 1
        assert run.results[0].status == CheckStatus.FAILED

    async def test_validate_multiple_traits(
        self, service, mock_trait_repo, mock_runner, validation_inputs
    ):
        """Test validation with multiple traits."""
        trait1 = make_trait("si-units")
        trait2 = make_trait("iso-dates")

        mock_trait_repo.get_or_fetch.side_effect = [trait1, trait2]
        mock_runner.run.side_effect = [
            ValidatorOutput(status=CheckStatus.PASSED, checks=[]),
            ValidatorOutput(status=CheckStatus.PASSED, checks=[]),
        ]

        trait_srns = [trait1.srn, trait2.srn]
        run = await service.validate(trait_srns, validation_inputs)

        assert run.status == RunStatus.COMPLETED
        assert len(run.results) == 2
        assert all(r.status == CheckStatus.PASSED for r in run.results)

    async def test_validate_runner_exception(
        self, service, mock_trait_repo, mock_runner, validation_inputs
    ):
        """Test that runner exceptions are captured as ERROR status."""
        trait = make_trait("si-units")
        mock_trait_repo.get_or_fetch.return_value = trait
        mock_runner.run.side_effect = Exception("Container failed to start")

        trait_srns = [trait.srn]
        run = await service.validate(trait_srns, validation_inputs)

        assert run.status == RunStatus.FAILED
        assert len(run.results) == 1
        assert run.results[0].status == CheckStatus.ERROR
        assert "Container failed to start" in run.results[0].message

    async def test_validate_saves_run_states(
        self, service, mock_trait_repo, mock_run_repo, mock_runner, validation_inputs
    ):
        """Test that the service saves run state at each stage."""
        trait = make_trait("si-units")
        mock_trait_repo.get_or_fetch.return_value = trait
        mock_runner.run.return_value = ValidatorOutput(
            status=CheckStatus.PASSED, checks=[]
        )

        # Track status at each save call
        statuses_seen = []

        def capture_status(run):
            statuses_seen.append(run.status)

        mock_run_repo.save.side_effect = capture_status

        trait_srns = [trait.srn]
        await service.validate(trait_srns, validation_inputs)

        # Should save: initial (pending), running, completed
        assert mock_run_repo.save.call_count == 3
        assert statuses_seen == [RunStatus.PENDING, RunStatus.RUNNING, RunStatus.COMPLETED]

    async def test_validate_stores_trait_srns(
        self, service, mock_trait_repo, mock_run_repo, mock_runner, validation_inputs
    ):
        """Test that the validation run stores the trait SRNs."""
        trait1 = make_trait("si-units")
        trait2 = make_trait("iso-dates")

        mock_trait_repo.get_or_fetch.side_effect = [trait1, trait2]
        mock_runner.run.side_effect = [
            ValidatorOutput(status=CheckStatus.PASSED, checks=[]),
            ValidatorOutput(status=CheckStatus.PASSED, checks=[]),
        ]

        trait_srns = [trait1.srn, trait2.srn]
        run = await service.validate(trait_srns, validation_inputs)

        assert run.trait_srns == trait_srns
