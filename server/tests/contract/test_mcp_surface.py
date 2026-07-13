"""DB-free contract tests for the MCP Apps surface at /mcp (#162).

Runs the REAL protocol stack — low-level MCP server, stateless streamable-HTTP
transport, Dishka UOW dispatch, domain view handlers — against fake read
stores injected through the same provider-override seam production hosts use
(`create_container(extra_providers...)`). No Postgres anywhere.

The seeded schema is deliberately generic (``sample-data`` with a
``measurements`` feature table): nothing in the surface may depend on any
domain specifics.
"""

import os
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from dishka import provide
from httpx import ASGITransport, AsyncClient

os.environ.setdefault("OSA_BASE_URL", "http://localhost:8000")
os.environ.setdefault("OSA_AUTH__JWT__SECRET", "test-secret-for-contract-tests-minimum-32-chars")

from osa.application.api.mcp.server import McpSurface  # noqa: E402
from osa.application.di import create_container  # noqa: E402
from osa.config import Config  # noqa: E402
from osa.domain.data.model.catalog import (  # noqa: E402
    CatalogEntry,
    NodeCatalog,
    TableResourceSummary,
)
from osa.domain.data.model.manifest import (  # noqa: E402
    IMPLICIT_FEATURE_COLUMN_SPECS,
    IMPLICIT_RECORD_COLUMN_SPECS,
    ColumnSpec,
    FieldSpec,
    SchemaManifest,
    TableResource,
)
from osa.domain.data.model.query_plan import (  # noqa: E402
    QueryPlan,
    TableKind,
    decode_cursor,
)
from osa.domain.data.model.record_summary import RecordSummary  # noqa: E402
from osa.domain.data.model.skill import AuthorDocs, SampleValue  # noqa: E402
from osa.domain.data.port.data_read_store import (  # noqa: E402
    DataCatalogReadStore,
    DataTableReadStore,
)
from osa.domain.semantics.model.value import FieldType  # noqa: E402
from osa.domain.shared.model.ids import RecordId  # noqa: E402
from osa.domain.shared.model.srn import RecordSRN, SchemaId  # noqa: E402
from osa.util.di.base import Provider  # noqa: E402
from osa.util.di.scope import Scope  # noqa: E402

SCHEMA_ID = SchemaId.parse("sample-data@1.0.0")
SCHEMA_SRN = "urn:osa:localhost:schema:sample-data@1.0.0"
RECORD_ID = "0198aaaa-1111"
RECORD_SRN = f"urn:osa:localhost:rec:{RECORD_ID}@1"
RUN_ID = "0198bbbb-2222"  # every feature row's provenance FK (feature #145)

MEASUREMENT_ROWS = [
    {
        "id": i,
        "record_srn": RECORD_SRN,
        "run_id": RUN_ID,
        "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "x": float(i),
        "y": float(i * i),
        "label": "even" if i % 2 == 0 else "odd",
    }
    for i in range(1, 6)
]


def _manifest() -> SchemaManifest:
    return SchemaManifest(
        id="sample-data",
        version="1.0.0",
        srn=SCHEMA_SRN,
        title="Sample data",
        fields=[
            FieldSpec(name="name", type=FieldType.TEXT),
            FieldSpec(name="score", type=FieldType.NUMBER, unit="au"),
        ],
        table_resources=[
            TableResource(
                name="records",
                kind=TableKind.RECORDS,
                columns=[
                    *IMPLICIT_RECORD_COLUMN_SPECS,
                    ColumnSpec(name="name", type=FieldType.TEXT),
                    ColumnSpec(name="score", type=FieldType.NUMBER),
                ],
                row_count=1,
                formats=["", "csv", "csv.gz"],
            ),
            TableResource(
                name="measurements",
                kind=TableKind.FEATURE,
                columns=[
                    *IMPLICIT_FEATURE_COLUMN_SPECS,
                    ColumnSpec(name="x", type=FieldType.NUMBER),
                    ColumnSpec(name="y", type=FieldType.NUMBER),
                    ColumnSpec(name="label", type=FieldType.TEXT),
                ],
                row_count=len(MEASUREMENT_ROWS),
                formats=["", "csv", "csv.gz"],
            ),
        ],
    )


def _record() -> RecordSummary:
    return RecordSummary(
        id=RecordId(RECORD_ID),
        srn=RecordSRN.parse(RECORD_SRN),
        schema_id=SCHEMA_ID,
        version=1,
        metadata={"name": "sample-1", "score": 0.9},
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


class FakeTableStore:
    """In-memory DataTableReadStore honouring the feature keyset cursor."""

    def __init__(self) -> None:
        self.last_plan: QueryPlan | None = None

    def stream_rows(self, plan: QueryPlan, timeout: Any = None) -> AsyncIterator[Mapping[str, Any]]:
        self.last_plan = plan

        async def gen() -> AsyncIterator[Mapping[str, Any]]:
            if plan.table_kind == TableKind.RECORDS:
                yield _record().flatten()
                return
            rows = MEASUREMENT_ROWS
            if plan.pagination.cursor is not None:
                after = decode_cursor(plan.pagination.cursor.value)["id"]
                rows = [r for r in rows if r["id"] > after]
            for row in rows:
                yield row

        return gen()


class FakeCatalogStore:
    async def get_record_by_id(self, id: RecordId, version: int | None) -> RecordSummary | None:
        return _record() if id == RECORD_ID else None

    async def get_node_catalog(self) -> NodeCatalog:
        return NodeCatalog(
            node_domain="localhost",
            schemas=[
                CatalogEntry(
                    id="sample-data",
                    version="1.0.0",
                    srn=SCHEMA_SRN,
                    table_resources=[
                        TableResourceSummary(name="records", kind=TableKind.RECORDS),
                        TableResourceSummary(name="measurements", kind=TableKind.FEATURE),
                    ],
                )
            ],
        )

    async def get_schema_manifest(self, schema_id: SchemaId) -> SchemaManifest | None:
        return _manifest() if schema_id == SCHEMA_ID else None

    async def get_latest_schema_id(self, schema_short_id: str) -> SchemaId | None:
        return SCHEMA_ID if schema_short_id == "sample-data" else None

    async def get_author_docs(self, schema_id: SchemaId) -> AuthorDocs | None:
        return None

    async def sample_value(
        self, schema_id: SchemaId, table: str, column: str
    ) -> SampleValue | None:
        return None


class FakeDataStoresProvider(Provider):
    """Overrides the Postgres read stores — everything above them is real."""

    @provide(scope=Scope.APP)
    def table_store(self) -> DataTableReadStore:
        return FakeTableStore()

    @provide(scope=Scope.APP)
    def catalog_store(self) -> DataCatalogReadStore:
        return FakeCatalogStore()


HEADERS = {
    "Accept": "application/json, text/event-stream",
    "Content-Type": "application/json",
}


# The session manager's anyio task group must be entered and exited in the
# SAME asyncio task, and pytest-asyncio drives async fixtures' setup/teardown
# as separate tasks — so the lifespan is opened inside each test's own task
# via this context manager rather than a fixture.
@asynccontextmanager
async def mcp_client(
    tmp_path: Path, configure: Callable[[Config], None] | None = None
) -> AsyncIterator[AsyncClient]:
    for widget in ("dataset-overview", "table", "chart", "record", "filter-panel"):
        (tmp_path / f"{widget}.html").write_text(f"<!doctype html><html>{widget}</html>")
    config = Config()
    config.mcp.widget_bundle_dir = tmp_path
    if configure is not None:
        configure(config)
    container = create_container(FakeDataStoresProvider())
    surface = McpSurface(container, config)
    try:
        async with surface.lifespan():
            async with AsyncClient(
                transport=ASGITransport(app=surface), base_url="http://test"
            ) as http:
                yield http
    finally:
        await container.close()


async def _rpc(client: AsyncClient, method: str, params: dict[str, Any] | None = None) -> Any:
    body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}}
    response = await client.post("/mcp", json=body, headers=HEADERS)
    assert response.status_code == 200, response.text
    envelope = response.json()
    assert "error" not in envelope, envelope
    return envelope["result"]


async def _call_tool(client: AsyncClient, name: str, arguments: dict[str, Any]) -> Any:
    return await _rpc(client, "tools/call", {"name": name, "arguments": arguments})


class TestInitialize:
    async def test_handshake_carries_skill_instructions(self, tmp_path: Path):
        async with mcp_client(tmp_path) as client:
            result = await _rpc(
                client,
                "initialize",
                {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {"name": "test", "version": "0"},
                },
            )
            # Instructions are the rendered SKILL.md — they must mention the
            # seeded dataset so the connecting model is grounded immediately.
            assert "sample-data" in result["instructions"]
            assert result["capabilities"]["tools"] is not None


class TestToolsList:
    async def test_all_tools_listed_with_ui_meta(self, tmp_path: Path):
        async with mcp_client(tmp_path) as client:
            result = await _rpc(client, "tools/list")
            tools = {t["name"]: t for t in result["tools"]}
            assert set(tools) == {
                "list_datasets",
                "describe_dataset",
                "show_table",
                "show_chart",
                "show_record",
                "show_filter_panel",
                "fetch_page",
                "sample_values",
            }
            assert tools["show_table"]["_meta"]["ui"]["resourceUri"] == "ui://osa/table"
            assert "model" in tools["show_table"]["_meta"]["ui"]["visibility"]

    async def test_app_only_tools_hidden_from_model(self, tmp_path: Path):
        async with mcp_client(tmp_path) as client:
            result = await _rpc(client, "tools/list")
            for tool in result["tools"]:
                visibility = tool["_meta"]["ui"]["visibility"]
                if tool["name"] in ("fetch_page", "sample_values"):
                    assert "model" not in visibility, tool["name"]
                else:
                    assert "model" in visibility, tool["name"]

    async def test_filter_schema_embeds_real_filter_union(self, tmp_path: Path):
        async with mcp_client(tmp_path) as client:
            result = await _rpc(client, "tools/list")
            show_table = next(t for t in result["tools"] if t["name"] == "show_table")
            assert "Predicate" in show_table["inputSchema"]["$defs"]


class TestViewTools:
    async def test_list_datasets(self, tmp_path: Path):
        async with mcp_client(tmp_path) as client:
            result = await _call_tool(client, "list_datasets", {})
            assert result["_meta"]["ui"]["resourceUri"] == "ui://osa/dataset-overview"
            (dataset,) = result["structuredContent"]["datasets"]
            assert dataset["title"] == "Sample data"
            assert dataset["feature_tables"] == ["measurements"]

    async def test_describe_dataset_returns_manifest_without_widget(self, tmp_path: Path):
        async with mcp_client(tmp_path) as client:
            result = await _call_tool(client, "describe_dataset", {"schema": "sample-data"})
            assert result["structuredContent"]["id"] == "sample-data"
            assert "_meta" not in result or "ui" not in (result.get("_meta") or {})

    async def test_show_table_over_feature_table(self, tmp_path: Path):
        async with mcp_client(tmp_path) as client:
            result = await _call_tool(
                client, "show_table", {"schema": "sample-data", "table": "measurements"}
            )
            assert result["_meta"]["ui"]["resourceUri"] == "ui://osa/table"
            content = result["structuredContent"]
            assert len(content["rows"]) == len(MEASUREMENT_ROWS)
            assert content["query"]["table"] == "measurements"
            column_names = [c["name"] for c in content["columns"]]
            assert {"id", "record_srn", "x", "y", "label"} <= set(column_names)

    async def test_show_chart_returns_rows_and_chart_binding(self, tmp_path: Path):
        async with mcp_client(tmp_path) as client:
            result = await _call_tool(
                client,
                "show_chart",
                {
                    "schema": "sample-data",
                    "table": "measurements",
                    "x": "x",
                    "y": "y",
                    "kind": "line",
                },
            )
            assert result["_meta"]["ui"]["resourceUri"] == "ui://osa/chart"
            content = result["structuredContent"]
            assert content["kind"] == "line"
            assert content["page"]["rows"][0]["x"] == 1.0

    async def test_show_chart_unknown_column_is_tool_error(self, tmp_path: Path):
        async with mcp_client(tmp_path) as client:
            result = await _call_tool(
                client,
                "show_chart",
                {
                    "schema": "sample-data",
                    "table": "measurements",
                    "x": "bogus",
                    "y": "y",
                    "kind": "line",
                },
            )
            assert result["isError"] is True
            # The error must teach the model what IS available.
            assert "bogus" in result["content"][0]["text"]
            assert "x" in result["content"][0]["text"]

    async def test_show_record(self, tmp_path: Path):
        async with mcp_client(tmp_path) as client:
            result = await _call_tool(client, "show_record", {"id": RECORD_ID})
            assert result["_meta"]["ui"]["resourceUri"] == "ui://osa/record"
            content = result["structuredContent"]
            assert content["record"]["metadata"]["name"] == "sample-1"
            assert content["feature_tables"] == ["measurements"]

    async def test_show_filter_panel_derives_facets(self, tmp_path: Path):
        async with mcp_client(tmp_path) as client:
            result = await _call_tool(client, "show_filter_panel", {"schema": "sample-data"})
            assert result["_meta"]["ui"]["resourceUri"] == "ui://osa/filter-panel"
            facets = {f["field"]: f for f in result["structuredContent"]["facets"]}
            assert facets["metadata.score"]["kind"] == "range"
            assert facets["metadata.name"]["kind"] == "text"

    async def test_unknown_schema_is_tool_error_not_protocol_error(self, tmp_path: Path):
        async with mcp_client(tmp_path) as client:
            result = await _call_tool(client, "show_table", {"schema": "nope"})
            assert result["isError"] is True

    async def test_invalid_filter_rejected_at_boundary(self, tmp_path: Path):
        async with mcp_client(tmp_path) as client:
            result = await _call_tool(
                client,
                "show_table",
                {
                    "schema": "sample-data",
                    "filter": {"kind": "predicate", "field": "bogus.path", "op": "eq", "value": 1},
                },
            )
            assert result["isError"] is True


class TestAppOnlyTools:
    async def test_fetch_page_cursor_round_trip(self, tmp_path: Path):
        async with mcp_client(tmp_path) as client:
            first = await _call_tool(
                client,
                "fetch_page",
                {"schema": "sample-data", "table": "measurements", "limit": 2},
            )
            page1 = first["structuredContent"]
            assert [r["id"] for r in page1["rows"]] == [1, 2]
            assert page1["truncated"] is True

            second = await _call_tool(
                client,
                "fetch_page",
                {
                    "schema": "sample-data",
                    "table": "measurements",
                    "limit": 2,
                    "cursor": page1["next_cursor"],
                },
            )
            page2 = second["structuredContent"]
            assert [r["id"] for r in page2["rows"]] == [3, 4]

    async def test_sample_values_dedupes(self, tmp_path: Path):
        async with mcp_client(tmp_path) as client:
            result = await _call_tool(
                client,
                "sample_values",
                {"schema": "sample-data", "table": "measurements", "column": "label"},
            )
            assert sorted(result["structuredContent"]["values"]) == ["even", "odd"]


class TestUiResources:
    async def test_resources_list_all_widgets(self, tmp_path: Path):
        async with mcp_client(tmp_path) as client:
            result = await _rpc(client, "resources/list")
            uris = {r["uri"] for r in result["resources"]}
            assert uris == {
                "ui://osa/dataset-overview",
                "ui://osa/table",
                "ui://osa/chart",
                "ui://osa/record",
                "ui://osa/filter-panel",
            }
            for resource in result["resources"]:
                assert resource["mimeType"] == "text/html;profile=mcp-app"

    async def test_read_widget_bundle(self, tmp_path: Path):
        async with mcp_client(tmp_path) as client:
            result = await _rpc(client, "resources/read", {"uri": "ui://osa/table"})
            (contents,) = result["contents"]
            assert contents["mimeType"] == "text/html;profile=mcp-app"
            assert "<!doctype html>" in contents["text"]
            assert contents["_meta"]["ui"]["csp"]["connectDomains"] == []


class TestTransportSecurity:
    async def test_protection_enabled_rejects_disallowed_host(self, tmp_path: Path):
        def enable(config: Config) -> None:
            config.mcp.dns_rebinding_protection = True
            config.mcp.allowed_hosts = ["allowed.example:*"]

        async with mcp_client(tmp_path, configure=enable) as client:
            response = await client.post(
                "/mcp",
                json={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}},
                headers=HEADERS,
            )
        # httpx sends Host: test — not on the allow-list → 421 Misdirected.
        assert response.status_code == 421

    async def test_protection_enabled_accepts_allowed_host(self, tmp_path: Path):
        def enable(config: Config) -> None:
            config.mcp.dns_rebinding_protection = True
            config.mcp.allowed_hosts = ["test:*", "test"]

        async with mcp_client(tmp_path, configure=enable) as client:
            result = await _rpc(client, "tools/list")
        assert result["tools"]


class TestAppRouting:
    def test_mcp_route_registered_on_app(self):
        from osa.application.api.rest.app import create_app

        app = create_app()
        assert "/mcp" in [r.path for r in app.routes if hasattr(r, "path")]

    def test_mcp_can_be_disabled(self, monkeypatch: pytest.MonkeyPatch):
        from osa.application.api.rest.app import create_app

        monkeypatch.setenv("OSA_MCP__ENABLED", "false")
        app = create_app()
        assert "/mcp" not in [r.path for r in app.routes if hasattr(r, "path")]
