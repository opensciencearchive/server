from datetime import datetime, timezone

from osa.domain.shared.model.srn import Domain, LocalId, Semver, TraitSRN, ValidationRunSRN
from osa.domain.validation.model import (
    CheckResult,
    CheckStatus,
    RunStatus,
    Trait,
    TraitStatus,
    ValidationRun,
    Validator,
    ValidatorLimits,
    ValidatorRef,
)


class TestValidatorRef:
    def test_create_validator_ref(self):
        ref = ValidatorRef(
            image="ghcr.io/osap/validators/si-units",
            digest="sha256:abc123",
        )
        assert ref.image == "ghcr.io/osap/validators/si-units"
        assert ref.digest == "sha256:abc123"


class TestValidatorLimits:
    def test_default_limits(self):
        limits = ValidatorLimits()
        assert limits.timeout_seconds == 60
        assert limits.memory == "256Mi"
        assert limits.cpu == "0.5"

    def test_custom_limits(self):
        limits = ValidatorLimits(
            timeout_seconds=120,
            memory="512Mi",
            cpu="1.0",
        )
        assert limits.timeout_seconds == 120
        assert limits.memory == "512Mi"
        assert limits.cpu == "1.0"


class TestValidator:
    def test_create_validator_with_defaults(self):
        ref = ValidatorRef(image="test/image", digest="sha256:123")
        validator = Validator(ref=ref)

        assert validator.ref == ref
        assert validator.limits.timeout_seconds == 60
        assert validator.limits.memory == "256Mi"

    def test_create_validator_with_custom_limits(self):
        ref = ValidatorRef(image="test/image", digest="sha256:123")
        limits = ValidatorLimits(timeout_seconds=300, memory="1Gi", cpu="2.0")
        validator = Validator(ref=ref, limits=limits)

        assert validator.ref == ref
        assert validator.limits.timeout_seconds == 300
        assert validator.limits.memory == "1Gi"


class TestTrait:
    def test_create_trait(self):
        trait_srn = TraitSRN(
            domain=Domain("osap.org"),
            id=LocalId("si-units"),
            version=Semver("1.0.0"),
        )
        validator = Validator(
            ref=ValidatorRef(image="ghcr.io/osap/si-units", digest="sha256:abc"),
        )
        now = datetime.now(timezone.utc)

        trait = Trait(
            srn=trait_srn,
            slug="si-units",
            name="SI Units Compliance",
            description="Validates that all measurements use SI units",
            validator=validator,
            status=TraitStatus.DRAFT,
            created_at=now,
        )

        assert trait.srn == trait_srn
        assert trait.slug == "si-units"
        assert trait.name == "SI Units Compliance"
        assert trait.status == TraitStatus.DRAFT
        assert trait.validator.ref.image == "ghcr.io/osap/si-units"

    def test_trait_status_values(self):
        assert TraitStatus.DRAFT == "draft"
        assert TraitStatus.ACTIVE == "active"
        assert TraitStatus.DEPRECATED == "deprecated"


class TestCheckResult:
    def test_create_check_result_passed(self):
        result = CheckResult(
            trait_srn="urn:osa:osap.org:trait:si-units@1.0.0",
            validator_digest="sha256:abc123",
            status=CheckStatus.PASSED,
        )
        assert result.status == CheckStatus.PASSED
        assert result.message is None
        assert result.details is None

    def test_create_check_result_failed_with_message(self):
        result = CheckResult(
            trait_srn="urn:osa:osap.org:trait:si-units@1.0.0",
            validator_digest="sha256:abc123",
            status=CheckStatus.FAILED,
            message="Temperature column uses Fahrenheit instead of Kelvin",
            details={"column": "temperature", "found": "F", "expected": "K"},
        )
        assert result.status == CheckStatus.FAILED
        assert "Fahrenheit" in result.message
        assert result.details["column"] == "temperature"

    def test_check_status_values(self):
        assert CheckStatus.PASSED == "passed"
        assert CheckStatus.WARNINGS == "warnings"
        assert CheckStatus.FAILED == "failed"
        assert CheckStatus.ERROR == "error"


class TestValidationRun:
    def test_create_validation_run(self):
        run_srn = ValidationRunSRN(
            domain=Domain("localhost"),
            id=LocalId("run-123"),
            version=None,
        )
        trait_srn = TraitSRN(
            domain=Domain("osap.org"),
            id=LocalId("si-units"),
            version=Semver("1.0.0"),
        )

        run = ValidationRun(
            srn=run_srn,
            trait_srns=[trait_srn],
            status=RunStatus.PENDING,
        )

        assert run.srn == run_srn
        assert run.trait_srns == [trait_srn]
        assert run.status == RunStatus.PENDING
        assert run.results == []
        assert run.started_at is None
        assert run.completed_at is None

    def test_validation_run_with_results(self):
        run_srn = ValidationRunSRN(
            domain=Domain("localhost"),
            id=LocalId("run-123"),
            version=None,
        )
        trait_srns = [
            TraitSRN(domain=Domain("osap.org"), id=LocalId("si-units"), version=Semver("1.0.0")),
            TraitSRN(domain=Domain("osap.org"), id=LocalId("iso-dates"), version=Semver("1.0.0")),
        ]
        now = datetime.now(timezone.utc)

        results = [
            CheckResult(
                trait_srn="urn:osa:osap.org:trait:si-units@1.0.0",
                validator_digest="sha256:abc",
                status=CheckStatus.PASSED,
            ),
            CheckResult(
                trait_srn="urn:osa:osap.org:trait:iso-dates@1.0.0",
                validator_digest="sha256:def",
                status=CheckStatus.FAILED,
                message="Invalid date format",
            ),
        ]

        run = ValidationRun(
            srn=run_srn,
            trait_srns=trait_srns,
            status=RunStatus.COMPLETED,
            results=results,
            started_at=now,
            completed_at=now,
        )

        assert len(run.results) == 2
        assert run.results[0].status == CheckStatus.PASSED
        assert run.results[1].status == CheckStatus.FAILED

    def test_run_status_values(self):
        assert RunStatus.PENDING == "pending"
        assert RunStatus.RUNNING == "running"
        assert RunStatus.COMPLETED == "completed"
        assert RunStatus.FAILED == "failed"
