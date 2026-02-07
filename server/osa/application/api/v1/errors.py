"""Centralized error transformation for API routes.

Maps OSA errors (domain and infrastructure) to HTTPException responses.
"""

from typing import Any

from fastapi import HTTPException

from osa.domain.shared.error import (
    AuthorizationError,
    ConflictError,
    DomainError,
    InfrastructureError,
    InvalidStateError,
    NotFoundError,
    OSAError,
    ValidationError,
)

DOMAIN_ERROR_STATUS_MAP: dict[type[DomainError], int] = {
    NotFoundError: 404,
    ValidationError: 422,
    InvalidStateError: 409,
    ConflictError: 409,
    AuthorizationError: 403,
}


def map_osa_error(error: OSAError) -> HTTPException:
    """Map an OSA error to an HTTPException.

    Args:
        error: The OSA error to map.

    Returns:
        HTTPException with appropriate status code and detail.
    """
    detail: dict[str, Any] = {
        "code": error.code,
        "message": error.message,
    }

    if isinstance(error, InfrastructureError):
        # Infrastructure errors → 503 Service Unavailable
        return HTTPException(status_code=503, detail=detail)

    if isinstance(error, DomainError):
        status_code = DOMAIN_ERROR_STATUS_MAP.get(type(error), 400)
        if isinstance(error, ValidationError) and error.field is not None:
            detail["field"] = error.field
        # Distinguish 401 (unauthenticated) from 403 (unauthorized)
        if isinstance(error, AuthorizationError) and error.code == "missing_token":
            return HTTPException(
                status_code=401,
                detail=detail,
                headers={"WWW-Authenticate": "Bearer"},
            )
        return HTTPException(status_code=status_code, detail=detail)

    # Fallback for unknown OSAError subclasses
    return HTTPException(status_code=500, detail=detail)


# Alias for backwards compatibility
map_domain_error = map_osa_error
