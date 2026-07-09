"""Liveness and readiness endpoints (#158).

``GET /health`` is a cheap liveness probe (process is up). ``GET /ready`` runs
component checks — database, worker pool, and configured runner — and returns
``200`` only when no checked component is failing, ``503`` otherwise. Each
check is individually guarded so a failing dependency becomes a component
``error`` rather than a 500. Both routes are unauthenticated and excluded from
request tracing (see ``app.py``'s ``excluded_urls``).
"""

import asyncio
from typing import Literal

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter, Request, Response
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from osa.config import Config
from osa.infrastructure.event.worker import WorkerPool
from osa.infrastructure.k8s.health import check_k8s_health
from osa.infrastructure.logging import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["health"], route_class=DishkaRoute)

# Component checks must answer fast so /ready is a usable orchestrator probe.
_CHECK_TIMEOUT_S = 2.0


class HealthResponse(BaseModel):
    """Liveness payload."""

    status: Literal["ok"]
    version: str


class ComponentStatus(BaseModel):
    """One readiness component's verdict.

    ``ok`` — checked and healthy. ``error`` — checked and failing (``detail``
    carries the reason). ``unchecked`` — deliberately not probed (e.g. the OCI
    runner, which has no cheap health check).
    """

    status: Literal["ok", "error", "unchecked"]
    detail: str | None = None


class ReadyResponse(BaseModel):
    """Readiness payload: overall verdict plus per-component detail."""

    status: Literal["ready", "degraded"]
    version: str
    components: dict[str, ComponentStatus]


class ReadinessProbe:
    """Runs the per-component readiness checks for ``GET /ready``.

    A component is ``error`` on any exception — never a 500 out of the route.
    ``unchecked`` (e.g. the OCI runner) is neutral: it neither proves nor
    degrades readiness. Any *checked* component that errors degrades.
    """

    def __init__(self, config: Config, session: AsyncSession, pool: WorkerPool) -> None:
        self._config = config
        self._session = session
        self._pool = pool

    async def run(self, request: Request) -> dict[str, ComponentStatus]:
        return {
            "db": await self._check_db(),
            "workers": self._check_workers(),
            "runner": await self._check_runner(request),
        }

    async def _check_db(self) -> ComponentStatus:
        """``SELECT 1`` through the request's UOW session, time-boxed."""
        try:
            await asyncio.wait_for(
                self._session.execute(text("SELECT 1")), timeout=_CHECK_TIMEOUT_S
            )
            return ComponentStatus(status="ok")
        except Exception as exc:  # any failure is a component error, never a 500
            return ComponentStatus(status="error", detail=str(exc))

    def _check_workers(self) -> ComponentStatus:
        """Worker pool health.

        "ok" means the pool has started (at least one worker's background task
        is running) and every worker's task is still alive. Under a TestClient
        without the lifespan the pool is never started, so this reports
        ``error`` with "worker pool not running".
        """
        try:
            workers = self._pool.workers
            if not workers or not any(w.is_alive for w in workers):
                return ComponentStatus(status="error", detail="worker pool not running")
            dead = [w.name for w in workers if not w.is_alive]
            if dead:
                return ComponentStatus(
                    status="error", detail=f"workers not running: {', '.join(dead)}"
                )
            return ComponentStatus(status="ok")
        except Exception as exc:
            return ComponentStatus(status="error", detail=str(exc))

    async def _check_runner(self, request: Request) -> ComponentStatus:
        """Runner health.

        On the OCI backend a docker-socket probe is out of scope, so the runner
        is reported ``unchecked``. On the K8s backend the real infra check
        (``check_k8s_health``) runs, time-boxed — hooks cannot run without it,
        so an error here degrades readiness.
        """
        if self._config.runner.backend != "k8s":
            return ComponentStatus(status="unchecked", detail="oci runner is not health-checked")

        try:
            # Optional dependency: only importable when the k8s extra is installed,
            # which is guaranteed on the k8s backend.
            from kubernetes_asyncio.client import ApiClient, BatchV1Api, CoreV1Api

            container = request.app.state.dishka_container
            api_client = await container.get(ApiClient)
            k8s = self._config.runner.k8s
            await asyncio.wait_for(
                check_k8s_health(
                    BatchV1Api(api_client),
                    CoreV1Api(api_client),
                    namespace=k8s.namespace,
                    pvc_name=k8s.data_pvc_name,
                ),
                timeout=_CHECK_TIMEOUT_S,
            )
            return ComponentStatus(status="ok")
        except Exception as exc:
            return ComponentStatus(status="error", detail=str(exc))


@router.get("/health")
async def health(config: FromDishka[Config]) -> HealthResponse:
    """Liveness probe — always ``200`` while the process is serving."""
    return HealthResponse(status="ok", version=config.version)


@router.get("/ready")
async def ready(
    config: FromDishka[Config],
    session: FromDishka[AsyncSession],
    pool: FromDishka[WorkerPool],
    request: Request,
    response: Response,
) -> ReadyResponse:
    """Readiness probe: DB + worker-pool + runner checks.

    Returns ``200`` when every *checked* component is healthy (``unchecked``
    is neutral), ``503`` when any checked component errors.
    """
    components = await ReadinessProbe(config, session, pool).run(request)
    is_ready = all(c.status != "error" for c in components.values())
    response.status_code = 200 if is_ready else 503
    return ReadyResponse(
        status="ready" if is_ready else "degraded",
        version=config.version,
        components=components,
    )
