from osa.domain.shared.model.value import ValueObject


class ValidatorRef(ValueObject):
    """Immutable reference to an OCI validator image."""

    image: str  # e.g., ghcr.io/osap/validators/si-units
    digest: str  # e.g., sha256:def456...
