"""K8s API error classification.

Maps kubernetes-asyncio ApiException status codes to OSA error types.
"""

from osa.domain.shared.error import ConfigurationError, InfrastructureError, OSAError


def classify_api_error(exc: Exception) -> OSAError:
    """Classify a K8s API error by HTTP status code.

    - 403 → ConfigurationError (RBAC misconfiguration, not retried)
    - 404 → ConfigurationError (namespace/resource missing, not retried)
    - 500, 503 → InfrastructureError (transient, retried by outbox)
    - Other → InfrastructureError
    """
    status = getattr(exc, "status", 0)
    reason = getattr(exc, "reason", str(exc))

    if status == 403:
        return ConfigurationError(
            f"K8s RBAC permission denied: {reason}. "
            "Check ServiceAccount permissions for the OSA namespace."
        )
    if status == 404:
        return ConfigurationError(
            f"K8s resource not found: {reason}. Check that the namespace and resources exist."
        )
    return InfrastructureError(f"K8s API error ({status}): {reason}")
