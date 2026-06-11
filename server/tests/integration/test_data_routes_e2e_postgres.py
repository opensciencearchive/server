"""End-to-end HTTP tests for the /data/ routes against a real Postgres.

Drives the full stack — FastAPI routing, DishkaRoute DI, the streaming
response, the serializers, and the SQL — through an ASGI client. Critically
validates that the request-scoped DB session stays alive through
``StreamingResponse`` iteration (the DishkaRoute + streaming interaction).

Lifespan is intentionally NOT run (no worker pool needed): ``create_app``
attaches the Dishka container to app state, so DI resolves without it.

Skips unless OSA_DATABASE__URL points at PostgreSQL.
"""

import gzip
import os
from datetime import UTC, datetime

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from osa.domain.semantics.model.schema import Schema
from osa.domain.semantics.model.value import Cardinality, FieldDefinition, FieldType
from osa.domain.shared.model.hook import ColumnDef
from osa.domain.shared.model.srn import RecordSRN, SchemaId
from osa.infrastructure.persistence.feature_store import PostgresFeatureStore
from osa.infrastructure.persistence.metadata_store import PostgresMetadataStore
from osa.infrastructure.persistence.repository.schema import (
    PostgresSemanticsSchemaRepository,
)
from osa.infrastructure.persistence.tables import conventions_table

from tests.integration.conftest import seed_record

# create_app() reads Config() at import/call time; localhost domain needs a base URL.
os.environ.setdefault("OSA_BASE_URL", "http://localhost:8000")
os.environ.setdefault("OSA_AUTH__JWT__SECRET", "test-secret-for-integration-tests-minimum-32-chars")

if "postgresql" not in os.environ.get("OSA_DATABASE__URL", ""):
    pytest.skip("OSA_DATABASE__URL not set to PostgreSQL", allow_module_level=True)

SCHEMA = SchemaId.parse("compound@1.0.0")


def _fields() -> list[FieldDefinition]:
    return [
        FieldDefinition(
            name="species",
            type=FieldType.TEXT,
            required=True,
            cardinality=Cardinality.EXACTLY_ONE,
        ),
        FieldDefinition(
            name="mw",
            type=FieldType.NUMBER,
            required=False,
            cardinality=Cardinality.EXACTLY_ONE,
        ),
    ]


async def _seed(engine: AsyncEngine, session: AsyncSession, n: int) -> None:
    store = PostgresMetadataStore(engine, session)
    await store.ensure_table(SCHEMA, _fields())
    await PostgresSemanticsSchemaRepository(session).save(
        Schema(id=SCHEMA, title="compound", fields=_fields(), created_at=datetime.now(UTC))
    )
    for i in range(n):
        srn = RecordSRN.parse(f"urn:osa:localhost:rec:rec{i:03d}@1")
        await seed_record(
            engine,
            srn=str(srn),
            schema_id=SCHEMA.id.root,
            schema_version=SCHEMA.version.root,
            metadata={"species": "Homo sapiens", "mw": float(i)},
            published_at=datetime(2026, 1, 1 + i, tzinfo=UTC),
        )
        await store.insert(SCHEMA, srn, {"species": "Homo sapiens", "mw": float(i)})
    await session.commit()


HOOK = "chem_features"


async def _seed_feature(
    engine: AsyncEngine, session: AsyncSession, record_srn: RecordSRN, n: int
) -> None:
    """Register a hook on the schema and populate ``features.chem_features``."""
    await session.execute(
        conventions_table.insert().values(
            srn=f"urn:osa:localhost:conv:{HOOK}@1.0.0",
            title="compound conv",
            description=None,
            schema_id=SCHEMA.id.root,
            schema_version=SCHEMA.version.root,
            file_requirements={},
            hooks=[{"name": HOOK}],
            source=None,
            created_at=datetime.now(UTC),
        )
    )
    await session.commit()
    feature_store = PostgresFeatureStore(engine, session)
    await feature_store.create_table(
        HOOK,
        [
            ColumnDef(name="score", json_type="number", required=True),
            ColumnDef(name="label", json_type="string", required=False),
        ],
    )
    await feature_store.insert_features(
        HOOK,
        str(record_srn),
        [{"score": float(i), "label": f"l{i}"} for i in range(n)],
    )


@pytest.fixture
def client() -> AsyncClient:
    from osa.application.api.rest.app import create_app

    app = create_app()
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


@pytest.mark.asyncio
class TestDataRoutesE2E:
    async def test_records_csv_gz_streams_full_table(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession, client: AsyncClient
    ):
        await _seed(pg_engine, pg_session, 3)
        async with client:
            resp = await client.get("/api/v1/data/compound@1.0.0/records.csv.gz")
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("application/gzip")
        text = gzip.decompress(resp.content).decode()
        lines = [ln for ln in text.splitlines() if ln]
        assert lines[0].split(",")[:5] == ["id", "srn", "schema_id", "version", "created_at"]
        assert len(lines) == 1 + 3  # header + 3 rows

    async def test_records_json_default_page(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession, client: AsyncClient
    ):
        await _seed(pg_engine, pg_session, 3)
        async with client:
            resp = await client.get("/api/v1/data/compound@1.0.0/records")
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["rows"]) == 3
        assert body["next_cursor"] is None
        assert body["rows"][0]["species"] == "Homo sapiens"

    async def test_records_json_cursor_pagination(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession, client: AsyncClient
    ):
        await _seed(pg_engine, pg_session, 5)
        async with client:
            page1 = (await client.get("/api/v1/data/compound@1.0.0/records?limit=2")).json()
            assert len(page1["rows"]) == 2
            assert page1["next_cursor"] is not None
            page2 = (
                await client.get(
                    f"/api/v1/data/compound@1.0.0/records?limit=2&cursor={page1['next_cursor']}"
                )
            ).json()
        ids1 = {r["id"] for r in page1["rows"]}
        ids2 = {r["id"] for r in page2["rows"]}
        assert ids1.isdisjoint(ids2)  # no overlap across pages

    async def test_post_csv_with_filter(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession, client: AsyncClient
    ):
        await _seed(pg_engine, pg_session, 4)
        body = {
            "filter": {
                "kind": "predicate",
                "field": "metadata.mw",
                "op": "gte",
                "value": 2.0,
            }
        }
        async with client:
            resp = await client.post("/api/v1/data/compound@1.0.0/records.csv", json=body)
        assert resp.status_code == 200
        lines = [ln for ln in resp.text.splitlines() if ln]
        assert len(lines) == 1 + 2  # mw in {2.0, 3.0}

    async def test_catalog_and_manifest(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession, client: AsyncClient
    ):
        await _seed(pg_engine, pg_session, 1)
        async with client:
            catalog = (await client.get("/api/v1/data")).json()
            assert any(s["id"] == "compound" for s in catalog["schemas"])
            manifest = (await client.get("/api/v1/data/compound@1.0.0")).json()
        assert manifest["id"] == "compound"
        names = [t["name"] for t in manifest["table_resources"]]
        assert "records" in names

    async def test_record_by_id_has_id_and_srn(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession, client: AsyncClient
    ):
        await _seed(pg_engine, pg_session, 1)
        async with client:
            resp = await client.get("/api/v1/data/records/rec000")
        assert resp.status_code == 200
        body = resp.json()
        assert body["id"] == "rec000"
        assert body["srn"] == "urn:osa:localhost:rec:rec000@1"

    async def test_reserved_and_unknown_schema_404(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession, client: AsyncClient
    ):
        await _seed(pg_engine, pg_session, 1)
        async with client:
            assert (await client.get("/api/v1/data/records")).status_code == 404
            assert (await client.get("/api/v1/data/datasets")).status_code == 404
            assert (await client.get("/api/v1/data/nope@9.9.9")).status_code == 404

    async def test_record_by_id_version_pin(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession, client: AsyncClient
    ):
        store = PostgresMetadataStore(pg_engine, pg_session)
        await store.ensure_table(SCHEMA, _fields())
        await PostgresSemanticsSchemaRepository(pg_session).save(
            Schema(id=SCHEMA, title="compound", fields=_fields(), created_at=datetime.now(UTC))
        )
        for version, mw in ((1, 10.0), (2, 20.0)):
            srn = RecordSRN.parse(f"urn:osa:localhost:rec:dup@{version}")
            await seed_record(
                pg_engine,
                srn=str(srn),
                schema_id=SCHEMA.id.root,
                schema_version=SCHEMA.version.root,
                metadata={"species": "Homo sapiens", "mw": mw},
                published_at=datetime(2026, 1, version, tzinfo=UTC),
            )
            await store.insert(SCHEMA, srn, {"species": "Homo sapiens", "mw": mw})
        await pg_session.commit()

        async with client:
            v1 = (await client.get("/api/v1/data/records/dup@1")).json()
            v2 = (await client.get("/api/v1/data/records/dup@2")).json()
            latest = (await client.get("/api/v1/data/records/dup")).json()
        assert v1["srn"] == "urn:osa:localhost:rec:dup@1"
        assert v2["srn"] == "urn:osa:localhost:rec:dup@2"
        # No version → latest published (@2, published 2026-01-02).
        assert latest["srn"] == "urn:osa:localhost:rec:dup@2"


@pytest.mark.asyncio
class TestRecordsStreamingEdges:
    async def test_post_csv_gz_happy_path(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession, client: AsyncClient
    ):
        await _seed(pg_engine, pg_session, 4)
        body = {
            "filter": {
                "kind": "predicate",
                "field": "metadata.mw",
                "op": "gte",
                "value": 2.0,
            }
        }
        async with client:
            resp = await client.post("/api/v1/data/compound@1.0.0/records.csv.gz", json=body)
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("application/gzip")
        lines = [ln for ln in gzip.decompress(resp.content).decode().splitlines() if ln]
        assert lines[0].split(",")[:5] == ["id", "srn", "schema_id", "version", "created_at"]
        assert len(lines) == 1 + 2  # header + mw in {2.0, 3.0}

    async def test_empty_result_returns_gzipped_header_only(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession, client: AsyncClient
    ):
        await _seed(pg_engine, pg_session, 2)
        body = {
            "filter": {
                "kind": "predicate",
                "field": "metadata.species",
                "op": "eq",
                "value": "Nonexistent species",
            }
        }
        async with client:
            resp = await client.post("/api/v1/data/compound@1.0.0/records.csv.gz", json=body)
        assert resp.status_code == 200
        lines = [ln for ln in gzip.decompress(resp.content).decode().splitlines() if ln]
        assert len(lines) == 1  # header only, no data rows
        assert lines[0].split(",")[:5] == ["id", "srn", "schema_id", "version", "created_at"]

    async def test_invalid_filter_field_4xx_before_bytes(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession, client: AsyncClient
    ):
        await _seed(pg_engine, pg_session, 2)
        body = {
            "filter": {
                "kind": "predicate",
                "field": "metadata.nonexistent_field",
                "op": "eq",
                "value": "x",
            }
        }
        async with client:
            resp = await client.post("/api/v1/data/compound@1.0.0/records.csv.gz", json=body)
        # The error must be surfaced before any streamed bytes (pre-flight).
        assert 400 <= resp.status_code < 500
        assert not resp.headers.get("content-type", "").startswith("application/gzip")

    async def test_unknown_schema_404_before_bytes(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession, client: AsyncClient
    ):
        await _seed(pg_engine, pg_session, 1)
        async with client:
            resp = await client.get("/api/v1/data/nope@9.9.9/records.csv.gz")
        assert resp.status_code == 404
        assert not resp.headers.get("content-type", "").startswith("application/gzip")


@pytest.mark.asyncio
class TestRecordsJsonEdges:
    async def test_limit_over_max_is_clamped(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession, client: AsyncClient
    ):
        await _seed(pg_engine, pg_session, 3)
        async with client:
            resp = await client.get("/api/v1/data/compound@1.0.0/records?limit=5000")
        assert resp.status_code == 200  # clamped, not rejected
        assert len(resp.json()["rows"]) == 3

    async def test_sort_param_orders_rows(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession, client: AsyncClient
    ):
        await _seed(pg_engine, pg_session, 3)
        async with client:
            resp = await client.get(
                "/api/v1/data/compound@1.0.0/records?sort=created_at:asc,id:asc"
            )
        rows = resp.json()["rows"]
        # created_at ascending → rec000 (earliest) first.
        assert rows[0]["id"] == "rec000"
        assert [r["id"] for r in rows] == sorted(r["id"] for r in rows)

    async def test_post_json_with_filter(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession, client: AsyncClient
    ):
        await _seed(pg_engine, pg_session, 4)
        body = {"filter": {"kind": "predicate", "field": "metadata.mw", "op": "lt", "value": 2.0}}
        async with client:
            resp = await client.post("/api/v1/data/compound@1.0.0/records", json=body)
        assert resp.status_code == 200
        rows = resp.json()["rows"]
        assert {r["id"] for r in rows} == {"rec000", "rec001"}  # mw 0.0, 1.0

    async def test_boolean_filter_composition(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession, client: AsyncClient
    ):
        # Exercises AND / OR / NOT SQL compilation (incl. the NOT→coalesce path)
        # that the relocated filter engine carries over from discovery.
        await _seed(pg_engine, pg_session, 5)  # mw ∈ {0,1,2,3,4}
        body = {
            "filter": {
                "kind": "and",
                "operands": [
                    {
                        "kind": "or",
                        "operands": [
                            {
                                "kind": "predicate",
                                "field": "metadata.mw",
                                "op": "gte",
                                "value": 3.0,
                            },
                            {"kind": "predicate", "field": "metadata.mw", "op": "lt", "value": 1.0},
                        ],
                    },
                    {
                        "kind": "not",
                        "operand": {
                            "kind": "predicate",
                            "field": "metadata.mw",
                            "op": "eq",
                            "value": 4.0,
                        },
                    },
                ],
            }
        }
        async with client:
            resp = await client.post("/api/v1/data/compound@1.0.0/records", json=body)
        assert resp.status_code == 200
        rows = resp.json()["rows"]
        # OR(mw≥3, mw<1) = {0,3,4}; AND NOT(mw=4) → {0,3}.
        assert {r["id"] for r in rows} == {"rec000", "rec003"}


@pytest.mark.asyncio
class TestCatalogEdges:
    async def test_empty_node_returns_200_empty_schemas(self, client: AsyncClient):
        async with client:
            resp = await client.get("/api/v1/data")
        assert resp.status_code == 200
        body = resp.json()
        assert body["schemas"] == []
        assert "node_domain" in body


@pytest.mark.asyncio
class TestLegacySurfaceRemoved:
    """US6: the legacy read surface is gone — every old path 404s."""

    async def test_legacy_discovery_route_404(self, client: AsyncClient):
        async with client:
            resp = await client.get("/api/v1/discovery/records")
        assert resp.status_code == 404

    async def test_legacy_records_srn_route_404(self, client: AsyncClient):
        async with client:
            resp = await client.get("/api/v1/records/urn:osa:localhost:rec:anything@1")
        assert resp.status_code == 404

    async def test_legacy_search_route_404(self, client: AsyncClient):
        async with client:
            resp = await client.get("/api/v1/search/records?q=anything")
        assert resp.status_code == 404

    async def test_stats_has_no_index_field(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession, client: AsyncClient
    ):
        async with client:
            resp = await client.get("/api/v1/stats")
        assert resp.status_code == 200
        body = resp.json()
        assert "indexes" not in body
        assert body["data_url"] == "/api/v1/data"


@pytest.mark.asyncio
class TestPostRateLimit:
    async def test_eleventh_post_within_a_minute_is_429(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession, client: AsyncClient
    ):
        from osa.application.api.v1.routes.data._limiter import limiter

        # The limiter is a module singleton shared across tests — reset its
        # window so this test's count starts clean and doesn't leak outward.
        limiter.reset()
        await _seed(pg_engine, pg_session, 1)
        statuses = []
        async with client:
            for _ in range(11):  # limit is 10/minute on POST routes
                resp = await client.post("/api/v1/data/compound@1.0.0/records.csv", json={})
                statuses.append(resp.status_code)
        limiter.reset()
        assert statuses[:10] == [200] * 10
        assert statuses[10] == 429


@pytest.mark.asyncio
class TestFeatureRoutesE2E:
    async def test_feature_csv_gz_streams_full_table(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession, client: AsyncClient
    ):
        await _seed(pg_engine, pg_session, 1)
        srn = RecordSRN.parse("urn:osa:localhost:rec:rec000@1")
        await _seed_feature(pg_engine, pg_session, srn, 3)
        async with client:
            resp = await client.get(f"/api/v1/data/compound@1.0.0/{HOOK}.csv.gz")
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("application/gzip")
        lines = [ln for ln in gzip.decompress(resp.content).decode().splitlines() if ln]
        assert lines[0].split(",")[:3] == ["id", "record_srn", "created_at"]
        assert "score" in lines[0] and "label" in lines[0]
        assert len(lines) == 1 + 3  # header + 3 rows

    async def test_feature_post_csv_with_filter(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession, client: AsyncClient
    ):
        await _seed(pg_engine, pg_session, 1)
        srn = RecordSRN.parse("urn:osa:localhost:rec:rec000@1")
        await _seed_feature(pg_engine, pg_session, srn, 4)
        body = {
            "filter": {
                "kind": "predicate",
                "field": f"features.{HOOK}.score",
                "op": "gte",
                "value": 2.0,
            }
        }
        async with client:
            resp = await client.post(f"/api/v1/data/compound@1.0.0/{HOOK}.csv", json=body)
        assert resp.status_code == 200
        lines = [ln for ln in resp.text.splitlines() if ln]
        assert len(lines) == 1 + 2  # score in {2.0, 3.0}

    async def test_feature_json_default_page(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession, client: AsyncClient
    ):
        await _seed(pg_engine, pg_session, 1)
        srn = RecordSRN.parse("urn:osa:localhost:rec:rec000@1")
        await _seed_feature(pg_engine, pg_session, srn, 3)
        async with client:
            resp = await client.get(f"/api/v1/data/compound@1.0.0/{HOOK}")
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["rows"]) == 3
        assert body["rows"][0]["label"] == "l0"

    async def test_unknown_feature_404(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession, client: AsyncClient
    ):
        await _seed(pg_engine, pg_session, 1)
        async with client:
            resp = await client.get("/api/v1/data/compound@1.0.0/no_such_feature.csv.gz")
        assert resp.status_code == 404

    async def test_manifest_lists_feature_table(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession, client: AsyncClient
    ):
        await _seed(pg_engine, pg_session, 1)
        srn = RecordSRN.parse("urn:osa:localhost:rec:rec000@1")
        await _seed_feature(pg_engine, pg_session, srn, 2)
        async with client:
            manifest = (await client.get("/api/v1/data/compound@1.0.0")).json()
        feature = next(t for t in manifest["table_resources"] if t["name"] == HOOK)
        assert feature["kind"] == "feature"
        assert feature["row_count"] == 2
