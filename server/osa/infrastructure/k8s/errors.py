"""K8s API error classification.

Maps kubernetes-asyncio ApiException status codes to runtime-failure facts.
Disposition (retry / give up / abort) is the FailurePolicy's call, not ours.
"""

from osa.domain.shared.failure import FailureKind, RuntimeFailure


def classify_api_error(exc: Exception) -> RuntimeFailure:
    """Classify a K8s API error by HTTP status code.

    - 403 → RBAC (ServiceAccount misconfiguration)
    - 404 → CONFIG (namespace/resource missing)
    - anything else → RUNTIME (cluster pressure, API hiccup)
    """
    status = getattr(exc, "status", 0)
    reason = getattr(exc, "reason", str(exc))

    if status == 403:
        return RuntimeFailure(
            FailureKind.RBAC,
            f"K8s RBAC permission denied: {reason}. "
            "Check ServiceAccount permissions for the OSA namespace.",
        )
    if status == 404:
        return RuntimeFailure(
            FailureKind.CONFIG,
            f"K8s resource not found: {reason}. Check that the namespace and resources exist.",
        )
    return RuntimeFailure(FailureKind.RUNTIME, f"K8s API error ({status}): {reason}")
