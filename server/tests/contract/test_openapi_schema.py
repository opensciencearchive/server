"""DB-free contract test: the OpenAPI schema must build.

FastAPI generates the OpenAPI schema lazily on first request to ``/openapi.json``
(and ``/docs``/``/redoc``, and the #151 root-discovery doc advertises
``openapi_url``). A route returning a raw ``Response`` subclass under
``from __future__ import annotations`` makes FastAPI try to schematize the
stringified return annotation, which 500s during schema generation while the
routes themselves keep working — so the rest of the suite never caught it.

This is the durable backstop: it fails however the regression is reintroduced.
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

    # A fresh app per test so FastAPI's cached ``openapi_schema`` can't mask a
    # generation failure between tests.
    return create_app()


def test_openapi_schema_builds():
    """The tightest reproduction: schema generation must not raise."""
    schema = _app().openapi()
    assert schema["openapi"].startswith("3.")
    assert schema["paths"]


@pytest.mark.asyncio
async def test_openapi_json_endpoint_returns_200():
    """End-to-end proof the 500 is gone."""
    client = AsyncClient(transport=ASGITransport(app=_app()), base_url="http://test")
    async with client:
        resp = await client.get("/openapi.json")
    assert resp.status_code == 200
    assert resp.json()["openapi"].startswith("3.")
