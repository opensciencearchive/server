"""Error hierarchy for OSA.

Error layers:
- OSAError: Base class for all OSA errors
- DomainError: Business rule violations, validation failures (4xx responses)
- InfrastructureError: System-level failures like storage/network issues (503 responses)

These errors are mapped to HTTP responses by the global exception handler in app.py.
"""


class OSAError(Exception):
    """Base class for all OSA errors."""

    def __init__(self, message: str, code: str | None = None) -> None:
        self.message = message
        self.code = code or self.__class__.__name__
        super().__init__(message)


# =============================================================================
# Domain Errors (business logic violations - typically 4xx)
# =============================================================================


class DomainError(OSAError):
    """Base class for domain/business errors."""


class NotFoundError(DomainError):
    """Resource not found."""


class ValidationError(DomainError):
    """Input validation failed."""

    def __init__(self, message: str, field: str | None = None) -> None:
        super().__init__(message, code="VALIDATION_ERROR")
        self.field = field


class InvalidStateError(DomainError):
    """Operation not allowed in current state."""


class ConflictError(DomainError):
    """Resource already exists or version conflict."""


class AuthorizationError(DomainError):
    """User not authorized for this operation."""


# =============================================================================
# Infrastructure Errors (system-level failures - typically 503)
# =============================================================================


class InfrastructureError(OSAError):
    """Base class for infrastructure/system errors."""


class StorageUnavailableError(InfrastructureError):
    """Storage backend (database, object store) is unavailable."""


class ExternalServiceError(InfrastructureError):
    """External service (upstream node, validator) is unavailable or failed."""


class ConfigurationError(InfrastructureError):
    """System misconfiguration detected."""
