"""Contract tests for the unversioned ``GET /metrics`` endpoint (#158, P7).

The Prometheus registry is process-global (configure-once). These tests assert
the exposition format and the disabled-path gating without depending on which
earlier test configured the bootstrap.
"""

import os
from types import SimpleNamespace
from unittest.mock import patch

from fastapi.testclient import TestClient

from osa.application.api.rest.app import create_app
from osa.infrastructure.telemetry.setup import bootstrap

os.environ.setdefault(
    "OSA_AUTH__JWT__SECRET",
    "test-secret-that-is-at-least-32-characters-long",
)
os.environ.setdefault("OSA_BASE_URL", "http://localhost:8000")

_PATCH = patch("osa.application.api.rest.app.validate_all_handlers")


def setup_module(module):  # noqa: ANN001, ANN201
    _PATCH.start()


def teardown_module(module):  # noqa: ANN001, ANN201
    _PATCH.stop()


class TestMetricsEnabled:
    def test_metrics_serves_prometheus_text(self):
        app = create_app()
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/metrics")
        assert resp.status_code == 200
        # Prometheus exposition is text/plain (with version + charset params).
        assert resp.headers["content-type"].startswith("text/plain")
        # Only assert the OSA metric family if this process actually configured
        # a prometheus registry (default-enabled, but configure-once per process).
        if bootstrap.prometheus_registry is not None:
            assert "osa_" in resp.text or resp.text == "" or "# HELP" in resp.text

    def test_repeated_create_app_metrics_stays_200(self):
        # The configure-once guard + owned registry must survive a second app.
        app1 = create_app()
        app2 = create_app()
        for app in (app1, app2):
            client = TestClient(app, raise_server_exceptions=False)
            resp = client.get("/metrics")
            assert resp.status_code == 200


class TestMetricsDisabled:
    def test_disabled_registry_returns_404(self):
        app = create_app()
        client = TestClient(app, raise_server_exceptions=False)
        # Simulate prometheus disabled by swapping the module-level bootstrap
        # for a stub whose owned registry is None.
        stub = SimpleNamespace(prometheus_registry=None)
        with patch("osa.application.api.v1.routes.metrics.bootstrap", stub):
            resp = client.get("/metrics")
        assert resp.status_code == 404
        assert "prometheus" in resp.json()["detail"].lower()


class TestTracingExclusions:
    def test_health_ready_metrics_excluded_from_tracing(self):
        with patch("osa.application.api.rest.app.logfire.instrument_fastapi") as mock:
            create_app()
        _, kwargs = mock.call_args
        excluded = kwargs["excluded_urls"]
        assert "/api/v1/health" in excluded
        assert "/api/v1/ready" in excluded
        assert "/metrics" in excluded
