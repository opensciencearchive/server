"""DB-free contract tests for the unified ``/data/`` read surface (US1–US6).

Runs in CI's ``server-contract`` job, which has **no** Postgres. Everything here
is asserted against routing + the OpenAPI surface without touching the database:

* the legacy read routes (``/discovery``, ``/records/{srn}``, ``/search``) are
  gone — Starlette resolves an unregistered path to 404 before any DI/DB runs;
* the new ``/data/`` operation IDs are registered with stable, unique names
  (the SDK-codegen contract).

DB-backed behaviour of the surface lives in ``tests/integration/test_data_*``.
"""

import os

import pytest
from httpx import ASGITransport, AsyncClient

# create_app() reads Config() at call time; provide the env it needs so this
# test is self-sufficient regardless of how the contract job is configured.
os.environ.setdefault("OSA_BASE_URL", "http://localhost:8000")
os.environ.setdefault("OSA_AUTH__JWT__SECRET", "test-secret-for-contract-tests-minimum-32-chars")


def _app():
    from osa.application.api.rest.app import create_app

    return create_app()


@pytest.fixture
def client() -> AsyncClient:
    # No lifespan → no worker pool, no DB connection; pure ASGI routing.
    return AsyncClient(transport=ASGITransport(app=_app()), base_url="http://test")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/discovery/records",
        "/api/v1/records/urn:osa:localhost:rec:anything@1",
        "/api/v1/search/records?q=anything",
    ],
)
async def test_legacy_read_routes_are_gone(client: AsyncClient, path: str):
    async with client:
        resp = await client.get(path)
    assert resp.status_code == 404


def test_data_surface_operation_ids_registered():
    # The factory-minted table op IDs must exist and be unique (SDK codegen).
    app = _app()
    op_ids = [oid for route in app.routes if (oid := getattr(route, "operation_id", None))]
    expected = {
        "data_get_node_catalog",
        "data_get_record_by_id",
        "records_get_json",
        "records_post_json",
        "records_get_csv",
        "records_post_csv",
        "records_get_csv_gz",
        "records_post_csv_gz",
        "feature_get_json",
        "feature_post_json",
        "feature_get_csv",
        "feature_post_csv",
        "feature_get_csv_gz",
        "feature_post_csv_gz",
    }
    assert expected <= set(op_ids)
    # No duplicates among factory-minted IDs.
    factory_ids = [o for o in op_ids if o in expected]
    assert len(factory_ids) == len(set(factory_ids))
