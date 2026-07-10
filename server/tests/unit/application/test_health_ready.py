"""Contract tests for the liveness/readiness endpoints (#158, P7).

Drives ``/api/v1/health`` and ``/api/v1/ready`` through a TestClient built by
``create_app``. The readiness component checks (db, workers, runner) are steered
deterministically with ``@provide(..., override=True)`` providers so both the
ready and degraded paths can be exercised without a real Postgres or a started
worker pool.
"""

import os
from types import SimpleNamespace
from unittest.mock import patch

from dishka import provide
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession

from osa.application.api.rest.app import create_app
from osa.config import Config
from osa.infrastructure.event.worker import WorkerPool
from osa.util.di.base import Provider
from osa.util.di.scope import Scope

os.environ.setdefault(
    "OSA_AUTH__JWT__SECRET",
    "test-secret-that-is-at-least-32-characters-long",
)
os.environ.setdefault("OSA_BASE_URL", "http://localhost:8000")


# ---------------------------------------------------------------------------
# Fakes + override providers
# ---------------------------------------------------------------------------


class _OkSession:
    """Session whose SELECT 1 succeeds."""

    async def execute(self, *args, **kwargs):  # noqa: ANN002, ANN003, ANN201
        return None


class _FailSession:
    """Session whose SELECT 1 raises — drives the degraded db path."""

    async def execute(self, *args, **kwargs):  # noqa: ANN002, ANN003, ANN201
        raise RuntimeError("connection refused")


def _alive_pool() -> WorkerPool:
    pool = WorkerPool()
    pool._workers.append(SimpleNamespace(is_alive=True, name="FakeWorker"))
    return pool


class OkDbProvider(Provider):
    @provide(scope=Scope.UOW, override=True)
    def get_session(self) -> AsyncSession:
        return _OkSession()  # type: ignore[return-value]


class FailDbProvider(Provider):
    @provide(scope=Scope.UOW, override=True)
    def get_session(self) -> AsyncSession:
        return _FailSession()  # type: ignore[return-value]


class AlivePoolProvider(Provider):
    @provide(scope=Scope.APP, override=True)
    def get_worker_pool(self) -> WorkerPool:
        return _alive_pool()


# Skip handler auth validation (other modules define handlers without __auth__).
_PATCH = patch("osa.application.api.rest.app.validate_all_handlers")


def setup_module(module):  # noqa: ANN001, ANN201
    _PATCH.start()


def teardown_module(module):  # noqa: ANN001, ANN201
    _PATCH.stop()


# ---------------------------------------------------------------------------
# /health
# ---------------------------------------------------------------------------


class TestHealth:
    def test_health_reports_ok_and_config_version(self):
        app = create_app()
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/api/v1/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"
        # Not the old hardcoded "0.1.0" — the real running version.
        assert body["version"] == Config().version


# ---------------------------------------------------------------------------
# /ready
# ---------------------------------------------------------------------------


class TestReady:
    def test_all_ok_is_ready_200(self):
        app = create_app(providers=[OkDbProvider(), AlivePoolProvider()])
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/api/v1/ready")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ready"
        assert set(body["components"]) == {"db", "workers", "runner"}
        assert body["components"]["db"]["status"] == "ok"
        assert body["components"]["workers"]["status"] == "ok"
        # OCI is the default backend — runner is unchecked, not an error.
        assert body["components"]["runner"]["status"] == "unchecked"
        assert body["version"] == Config().version

    def test_runner_unchecked_does_not_degrade(self):
        app = create_app(providers=[OkDbProvider(), AlivePoolProvider()])
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/api/v1/ready")
        # unchecked runner + ok required components => still 200/ready.
        assert resp.status_code == 200
        assert resp.json()["components"]["runner"]["status"] == "unchecked"

    def test_db_failure_degrades_to_503(self):
        app = create_app(providers=[FailDbProvider(), AlivePoolProvider()])
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/api/v1/ready")
        assert resp.status_code == 503
        body = resp.json()
        assert body["status"] == "degraded"
        assert body["components"]["db"]["status"] == "error"
        assert body["components"]["db"]["detail"]  # carries the exception message
        # Other components are still reported alongside the failure.
        assert body["components"]["workers"]["status"] == "ok"
        assert body["components"]["runner"]["status"] == "unchecked"

    def test_unstarted_worker_pool_degrades(self):
        # Default (real) worker pool is un-started under TestClient (no lifespan).
        app = create_app(providers=[OkDbProvider()])
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/api/v1/ready")
        assert resp.status_code == 503
        body = resp.json()
        assert body["components"]["workers"]["status"] == "error"
        assert body["components"]["db"]["status"] == "ok"
