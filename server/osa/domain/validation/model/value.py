from enum import StrEnum

from osa.domain.shared.model.value import ValueObject


class RunStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class CheckStatus(StrEnum):
    PASSED = "passed"
    WARNINGS = "warnings"
    FAILED = "failed"
    ERROR = "error"


class ValidatorRef(ValueObject):
    """Immutable reference to an OCI validator image."""

    image: str  # e.g., ghcr.io/osap/validators/si-units
    digest: str  # e.g., sha256:def456...


class ValidatorLimits(ValueObject):
    """Recommended resource limits for running the validator."""

    timeout_seconds: int = 60
    memory: str = "256Mi"
    cpu: str = "0.5"


class Validator(ValueObject):
    """Complete specification for running an OCI validator."""

    ref: ValidatorRef
    limits: ValidatorLimits = ValidatorLimits()
