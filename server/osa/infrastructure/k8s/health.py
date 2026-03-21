"""Startup health check for K8s infrastructure."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from osa.domain.shared.error import ConfigurationError

if TYPE_CHECKING:
    from kubernetes_asyncio.client import BatchV1Api, CoreV1Api

logger = logging.getLogger(__name__)


async def check_k8s_health(
    batch_api: BatchV1Api,
    core_api: CoreV1Api,
    *,
    namespace: str,
    pvc_name: str,
) -> None:
    """Verify K8s infrastructure is ready for running Jobs.

    Checks:
    1. K8s API reachable and namespace exists (list_namespaced_job)
    2. RBAC permissions correct (same call)
    3. Data PVC exists (read_namespaced_persistent_volume_claim)

    Raises ConfigurationError with actionable message on failure.
    """
    # Check API reachability, namespace, and RBAC
    try:
        await batch_api.list_namespaced_job(namespace, limit=1)
    except Exception as exc:
        status = getattr(exc, "status", None)
        if status == 403:
            raise ConfigurationError(
                f"K8s RBAC permission denied in namespace '{namespace}'. "
                "Ensure the ServiceAccount can create/list/delete Jobs."
            ) from exc
        if status == 404:
            raise ConfigurationError(
                f"K8s namespace '{namespace}' not found. "
                "Create the namespace or update OSA_RUNNER__K8S__NAMESPACE."
            ) from exc
        raise ConfigurationError(
            f"K8s API unreachable: {exc}. Check cluster connectivity and kubeconfig."
        ) from exc

    # Check PVC existence
    try:
        await core_api.read_namespaced_persistent_volume_claim(pvc_name, namespace)
    except Exception as exc:
        status = getattr(exc, "status", None)
        if status == 404:
            raise ConfigurationError(
                f"PVC '{pvc_name}' not found in namespace '{namespace}'. "
                "Create the PVC or update OSA_RUNNER__K8S__DATA_PVC_NAME."
            ) from exc
        raise ConfigurationError(f"Failed to verify PVC '{pvc_name}': {exc}") from exc

    logger.info("K8s health check passed: namespace=%s, pvc=%s", namespace, pvc_name)
