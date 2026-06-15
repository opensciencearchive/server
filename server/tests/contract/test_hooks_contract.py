"""DB-free contract tests for the hook registry routes (#145, US3–US5).

Runs in CI's no-Postgres contract job. Asserts routing + auth gating without
touching the database: unauthenticated writes are rejected at handler
construction (the Principal provider raises ``missing_token`` → 401) before any
DI/DB work, and the new hook paths are registered on the app. DB-backed
behaviour (version monotonicity, idempotency, provenance) lives in the unit +
integration suites.
"""

import os

import pytest
from httpx import ASGITransport, AsyncClient

os.environ.setdefault("OSA_BASE_URL", "http://localhost:8000")
os.environ.setdefault("OSA_AUTH__JWT__SECRET", "test-secret-for-contract-tests-minimum-32-chars")


def _app():
    from osa.application.api.rest.app import create_app

    return create_app()


@pytest.fixture
def client() -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=_app()), base_url="http://test")


@pytest.mark.asyncio
async def test_create_release_requires_auth(client: AsyncClient):
    async with client:
        resp = await client.post(
            "/api/v1/hooks/pocket_detect/releases",
            json={"image": "reg/p:abc", "digest": "sha256:x", "config": {}, "source_ref": "git"},
        )
    assert resp.status_code == 401
    assert resp.json()["code"] == "missing_token"


@pytest.mark.asyncio
async def test_set_live_requires_auth(client: AsyncClient):
    async with client:
        resp = await client.put("/api/v1/hooks/pocket_detect/live", json={"version": 1})
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_create_release_rejects_unknown_field(client: AsyncClient):
    # Body validation runs before auth, so an unknown field 422s for any caller —
    # a payload-shape drift (e.g. the old `runtime` key) fails loudly at deploy.
    async with client:
        resp = await client.post(
            "/api/v1/hooks/pocket_detect/releases",
            json={"image": "i", "digest": "d", "config": {}, "source_ref": "g", "runtime": {}},
        )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_create_release_requires_config(client: AsyncClient):
    # A dropped `config` is rejected, never silently defaulted to {}.
    async with client:
        resp = await client.post(
            "/api/v1/hooks/pocket_detect/releases",
            json={"image": "i", "digest": "d", "source_ref": "g"},
        )
    assert resp.status_code == 422


def test_hook_routes_registered():
    app = _app()
    paths = {getattr(r, "path", None) for r in app.routes}
    assert "/api/v1/hooks" in paths
    assert "/api/v1/hooks/{name}/releases" in paths
    assert "/api/v1/hooks/{name}/releases/{version}" in paths
    assert "/api/v1/hooks/{name}/live" in paths
