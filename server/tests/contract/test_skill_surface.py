"""DB-free contract tests for the skill surface routing (#151, US1).

Asserts what routing alone can prove without Postgres: the unprefixed root
routes exist, the ``.md`` reference route is registered before the
``/{schema}`` manifest catch-all (longest-suffix-first, like ``.csv``), and
reserved ids 404 via ``resolve_schema``'s guard (which runs before any DB
query).

Full-response shape (root discovery JSON, SKILL.md frontmatter, reference
markdown vs JSON manifest) is DB-backed and lives in
``tests/integration/test_skill_surface_postgres.py``.
"""

import os

import pytest
from httpx import ASGITransport, AsyncClient

os.environ.setdefault("OSA_BASE_URL", "http://localhost:8000")
os.environ.setdefault("OSA_AUTH__JWT__SECRET", "test-secret-for-contract-tests-minimum-32-chars")


def _app():
    from osa.application.api.rest.app import create_app

    return create_app()


def _route_paths(app) -> list[str]:
    return [r.path for r in app.routes if hasattr(r, "path")]


class TestRootRoutesRegistered:
    def test_root_discovery_route_exists_unprefixed(self):
        assert "/" in _route_paths(_app())

    def test_skill_md_route_exists_unprefixed(self):
        assert "/SKILL.md" in _route_paths(_app())

    def test_reference_route_exists_on_data_surface(self):
        assert "/api/v1/data/{schema}.md" in _route_paths(_app())


class TestReferenceRouteOrdering:
    def test_md_route_registered_before_manifest_catch_all(self):
        # Route-ordering regression: `/{schema}.md` must precede `/{schema}`
        # or the manifest catch-all would capture `alloy-tests.md`.
        paths = _route_paths(_app())
        assert paths.index("/api/v1/data/{schema}.md") < paths.index("/api/v1/data/{schema}")


@pytest.mark.asyncio
class TestReservedReferenceIds:
    @pytest.mark.parametrize("reserved", ["records", "datasets", "runs"])
    async def test_reserved_ids_404(self, reserved: str):
        # resolve_schema rejects reserved names before touching the DB, so
        # this contract holds without Postgres (FR-024).
        client = AsyncClient(transport=ASGITransport(app=_app()), base_url="http://test")
        async with client:
            resp = await client.get(f"/api/v1/data/{reserved}.md")
        assert resp.status_code == 404
