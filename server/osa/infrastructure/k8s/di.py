"""Dishka DI provider for runner infrastructure (OCI or Kubernetes).

Uses Dishka's conditional activation (Marker + when=) to register only
the factories needed for the configured backend. When backend is "oci",
only Docker-related factories activate. When "k8s", only K8s factories
activate. No None placeholders, no unused dependencies resolved.
"""

import logging
from typing import AsyncIterable

import aiodocker
from dishka import activate, provide

from osa.config import Config
from osa.domain.shared.port.ingester_runner import IngesterRunner
from osa.domain.validation.port.hook_runner import HookRunner
from osa.infrastructure.oci.ingester_runner import OciIngesterRunner
from osa.infrastructure.oci.runner import OciHookRunner
from osa.infrastructure.s3.client import S3Client
from osa.util.di.base import Provider
from osa.util.di.markers import K8S
from osa.util.di.scope import Scope

try:
    from kubernetes_asyncio.client import ApiClient
except ImportError:
    ApiClient = object  # type: ignore[misc,assignment]

logger = logging.getLogger(__name__)


class RunnerProvider(Provider):
    """Config-driven runner provider.

    Uses Dishka conditional activation: factories decorated with
    ``when=K8S`` only activate when the activator returns True
    (i.e. ``config.runner.backend == "k8s"``). Undecorated factories
    serve as the default OCI path.
    """

    @activate(K8S)
    def is_k8s(self, config: Config) -> bool:
        return config.runner.backend == "k8s"

    # ------------------------------------------------------------------
    # OCI backend (default — no when= condition)
    # ------------------------------------------------------------------

    @provide(scope=Scope.APP)
    async def get_docker(self, config: Config) -> AsyncIterable[aiodocker.Docker]:
        docker = aiodocker.Docker()
        yield docker
        await docker.close()

    @provide(scope=Scope.UOW)
    def get_hook_runner_oci(
        self,
        docker: aiodocker.Docker,
        config: Config,
    ) -> HookRunner:
        return OciHookRunner(docker=docker, host_data_dir=config.host_data_dir)

    @provide(scope=Scope.UOW)
    def get_ingester_runner_oci(
        self,
        docker: aiodocker.Docker,
        config: Config,
    ) -> IngesterRunner:
        return OciIngesterRunner(docker=docker, host_data_dir=config.host_data_dir)

    # ------------------------------------------------------------------
    # K8s backend (activated when config.runner.backend == "k8s")
    # ------------------------------------------------------------------

    @provide(when=K8S, scope=Scope.APP)
    async def get_k8s_api_client(self, config: Config) -> AsyncIterable[ApiClient]:
        from osa.domain.shared.error import ConfigurationError

        try:
            import kubernetes_asyncio  # noqa: F401
        except ImportError:
            raise ConfigurationError(
                "kubernetes-asyncio is required for K8s runner. Install with: pip install osa[k8s]"
            )

        from kubernetes_asyncio import client as k8s_client
        from kubernetes_asyncio import config as k8s_config

        try:
            k8s_config.load_incluster_config()
        except k8s_config.ConfigException:
            await k8s_config.load_kube_config()

        api_client = k8s_client.ApiClient()

        # Startup health check
        from osa.infrastructure.k8s.health import check_k8s_health

        k8s_cfg = config.runner.k8s
        batch_api = k8s_client.BatchV1Api(api_client)
        core_api = k8s_client.CoreV1Api(api_client)
        await check_k8s_health(
            batch_api,
            core_api,
            namespace=k8s_cfg.namespace,
            pvc_name=k8s_cfg.data_pvc_name,
        )

        logger.info("K8s API client initialized (namespace=%s)", k8s_cfg.namespace)
        yield api_client
        await api_client.close()

    @provide(when=K8S, scope=Scope.APP)
    def get_s3_client(self, config: Config) -> S3Client:
        k8s = config.runner.k8s
        client = S3Client(
            bucket=k8s.s3_bucket,
            endpoint_url=k8s.s3_endpoint_url,
        )
        logger.info("S3 client initialized (bucket=%s)", k8s.s3_bucket)
        return client

    @provide(when=K8S, scope=Scope.UOW)
    def get_hook_runner_k8s(
        self,
        k8s_api_client: ApiClient,
        config: Config,
        s3: S3Client,
    ) -> HookRunner:
        from osa.infrastructure.k8s.runner import K8sHookRunner

        return K8sHookRunner(api_client=k8s_api_client, config=config.runner.k8s, s3=s3)

    @provide(when=K8S, scope=Scope.UOW)
    def get_ingester_runner_k8s(
        self,
        k8s_api_client: ApiClient,
        config: Config,
        s3: S3Client,
    ) -> IngesterRunner:
        from osa.infrastructure.k8s.ingester_runner import K8sIngesterRunner

        return K8sIngesterRunner(api_client=k8s_api_client, config=config.runner.k8s, s3=s3)
