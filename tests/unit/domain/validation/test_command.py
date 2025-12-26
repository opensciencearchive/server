from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from osa.domain.shared.model.srn import Domain, LocalId, Semver, TraitSRN
from osa.domain.validation.command import RegisterTrait, RegisterTraitHandler
from osa.domain.validation.model import (
    TraitStatus,
    Validator,
    ValidatorLimits,
    ValidatorRef,
)


class TestRegisterTraitHandler:
    @pytest.fixture
    def mock_trait_repo(self):
        return AsyncMock()

    @pytest.fixture
    def handler(self, mock_trait_repo):
        return RegisterTraitHandler(trait_repo=mock_trait_repo)

    @pytest.fixture
    def trait_srn(self):
        return TraitSRN(
            domain=Domain("localhost"),
            id=LocalId("test-trait"),
            version=Semver("1.0.0"),
        )

    @pytest.fixture
    def validator(self):
        return Validator(
            ref=ValidatorRef(
                image="ghcr.io/test/validator",
                digest="sha256:abc123",
            ),
            limits=ValidatorLimits(
                timeout_seconds=60,
                memory="256Mi",
                cpu="0.5",
            ),
        )

    async def test_register_trait_creates_trait(
        self, handler, mock_trait_repo, trait_srn, validator
    ):
        """Test that RegisterTrait creates a new trait."""
        cmd = RegisterTrait(
            srn=trait_srn,
            slug="test-trait",
            name="Test Trait",
            description="A trait for testing",
            validator=validator,
        )

        result = await handler.run(cmd)

        assert result.srn == trait_srn
        mock_trait_repo.save.assert_called_once()

        # Check the saved trait
        saved_trait = mock_trait_repo.save.call_args[0][0]
        assert saved_trait.srn == trait_srn
        assert saved_trait.slug == "test-trait"
        assert saved_trait.name == "Test Trait"
        assert saved_trait.description == "A trait for testing"
        assert saved_trait.validator == validator
        assert saved_trait.status == TraitStatus.DRAFT

    async def test_register_trait_sets_created_at(
        self, handler, mock_trait_repo, trait_srn, validator
    ):
        """Test that RegisterTrait sets created_at timestamp."""
        before = datetime.now(timezone.utc)

        cmd = RegisterTrait(
            srn=trait_srn,
            slug="test-trait",
            name="Test Trait",
            description="A trait for testing",
            validator=validator,
        )

        await handler.run(cmd)

        after = datetime.now(timezone.utc)
        saved_trait = mock_trait_repo.save.call_args[0][0]

        assert before <= saved_trait.created_at <= after

    async def test_register_trait_preserves_validator_config(
        self, handler, mock_trait_repo, trait_srn
    ):
        """Test that validator configuration is preserved."""
        custom_validator = Validator(
            ref=ValidatorRef(
                image="custom/image:latest",
                digest="sha256:customdigest",
            ),
            limits=ValidatorLimits(
                timeout_seconds=300,
                memory="1Gi",
                cpu="2.0",
            ),
        )

        cmd = RegisterTrait(
            srn=trait_srn,
            slug="custom-trait",
            name="Custom Trait",
            description="Trait with custom validator config",
            validator=custom_validator,
        )

        await handler.run(cmd)

        saved_trait = mock_trait_repo.save.call_args[0][0]
        assert saved_trait.validator.ref.image == "custom/image:latest"
        assert saved_trait.validator.ref.digest == "sha256:customdigest"
        assert saved_trait.validator.limits.timeout_seconds == 300
        assert saved_trait.validator.limits.memory == "1Gi"
        assert saved_trait.validator.limits.cpu == "2.0"
