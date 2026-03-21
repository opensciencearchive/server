"""Tests for K8s startup health check."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from osa.domain.shared.error import ConfigurationError
from osa.infrastructure.k8s.health import check_k8s_health


class _FakeApiException(Exception):
    def __init__(self, status: int, reason: str = ""):
        self.status = status
        self.reason = reason
        super().__init__(f"{status}: {reason}")


class TestCheckK8sHealth:
    @pytest.mark.asyncio
    async def test_healthy_cluster(self):
        batch_api = AsyncMock()
        core_api = AsyncMock()
        batch_api.list_namespaced_job.return_value = MagicMock(items=[])
        core_api.read_namespaced_persistent_volume_claim.return_value = MagicMock()

        await check_k8s_health(batch_api, core_api, namespace="osa", pvc_name="data-pvc")

    @pytest.mark.asyncio
    async def test_api_unreachable(self):
        batch_api = AsyncMock()
        core_api = AsyncMock()
        batch_api.list_namespaced_job.side_effect = Exception("Connection refused")

        with pytest.raises(ConfigurationError, match="K8s API"):
            await check_k8s_health(batch_api, core_api, namespace="osa", pvc_name="data-pvc")

    @pytest.mark.asyncio
    async def test_namespace_not_found(self):
        batch_api = AsyncMock()
        core_api = AsyncMock()
        batch_api.list_namespaced_job.side_effect = _FakeApiException(404, "Not Found")

        with pytest.raises(ConfigurationError, match="osa"):
            await check_k8s_health(batch_api, core_api, namespace="osa", pvc_name="data-pvc")

    @pytest.mark.asyncio
    async def test_rbac_forbidden(self):
        batch_api = AsyncMock()
        core_api = AsyncMock()
        batch_api.list_namespaced_job.side_effect = _FakeApiException(403, "Forbidden")

        with pytest.raises(ConfigurationError, match="permission"):
            await check_k8s_health(batch_api, core_api, namespace="osa", pvc_name="data-pvc")

    @pytest.mark.asyncio
    async def test_pvc_missing(self):
        batch_api = AsyncMock()
        core_api = AsyncMock()
        batch_api.list_namespaced_job.return_value = MagicMock(items=[])
        core_api.read_namespaced_persistent_volume_claim.side_effect = _FakeApiException(
            404, "Not Found"
        )

        with pytest.raises(ConfigurationError, match="data-pvc"):
            await check_k8s_health(batch_api, core_api, namespace="osa", pvc_name="data-pvc")
