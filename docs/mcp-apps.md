# MCP Apps surface (`/mcp`)

Every OSA node serves a [Model Context Protocol](https://modelcontextprotocol.io)
endpoint with the [MCP Apps extension](https://github.com/modelcontextprotocol/ext-apps)
(spec 2026-01-26) at `https://<node>/mcp`. Any MCP-Apps-capable host — Claude,
ChatGPT, Goose, VS Code — can connect, ask natural-language questions about the
node's published data, and render **interactive UI inline**: sortable tables,
charts, record-detail cards, and faceted filter panels.

Two properties fall out of the protocol choice:

- **No LLM key, no inference cost on the node.** The connecting user's own
  assistant is the orchestrator; the node ships tools + UI and never calls a
  model.
- **Domain-agnostic by construction.** Every tool operates over the live
  `/data/` catalog and manifests. All meaning comes from the schema field
  descriptions and author docs the node already serves — there is zero
  schema-specific code.

## Add a node as a connector

Point any MCP-Apps host at the streamable-HTTP endpoint:

```
https://<node-domain>/mcp
```

No authentication is required (the surface mirrors the public, read-only
`/data/` API). The server's `instructions` are the node's rendered `SKILL.md`,
so the model is grounded in the node's datasets the moment it connects.

## Tools

Model-visible (each view tool's result carries `_meta.ui.resourceUri` naming
the widget the host renders):

| Tool | Delegates to | Widget |
|---|---|---|
| `list_datasets` | `GetDatasetList` | `ui://osa/dataset-overview` |
| `describe_dataset` | `GetSchemaManifest` | — (data for the model) |
| `show_table` | `ReadTablePage` | `ui://osa/table` |
| `show_chart` | `ReadTablePage` | `ui://osa/chart` |
| `show_record` | `GetRecordDetail` | `ui://osa/record` |
| `show_filter_panel` | `GetFilterPanel` | `ui://osa/filter-panel` |

App-only (declared with `_meta.ui.visibility: ["app"]` — hosts keep them out
of the model's tool list; widgets invoke them via the host with **zero model
inference**, so a pagination click costs one DB query and no tokens):

| Tool | Purpose |
|---|---|
| `fetch_page` | next/prev page or re-sort for the table widget (`ReadTablePage`) |
| `sample_values` | bounded, deduped column sample for filter-panel facet options (`GetColumnSample`) |

All tools delegate to `domain/data` query handlers and inherit the same filter
grammar (`FilterExpr`), operator/type rules, filter-tree bounds, and page-limit
clamp as the REST `/data/` surface.

## Architecture

```
MCP host (runs the model, renders ui:// widgets in a sandboxed iframe)
   │ tool calls (streamable HTTP, stateless, JSON)
   ▼
application/api/mcp/      ← thin protocol adapter
   • Tool classes: ToolSpec + one domain handler binding each (import-time
     enforced); dispatch opens one anonymous UOW scope per call
   • ui://osa/* resource provider (compiled widget bundles, default-deny CSP)
   ▼
domain/data/query/view.py ← ReadTablePage / GetDatasetList / GetRecordDetail /
                            GetFilterPanel / GetColumnSample (public, DB-free
                            of MCP concepts — a future first-party canvas
                            consumes the same queries)
```

## Widgets

`packages/osa-widgets/` (TypeScript + React + Chart.js) compiles each widget to
**one self-contained HTML file** — all JS/CSS inlined, no external requests —
served via MCP `resources/read` as `text/html;profile=mcp-app`. The widgets
speak MCP-Apps postMessage JSON-RPC to the host (`src/protocol/` is the single
compatibility seam for the young spec).

Build and stage the bundles into the server package:

```bash
just widgets-build   # pnpm build → server/osa/application/api/mcp/bundles/
```

CI bakes the bundles into the server image (`image.yml`). A node without
bundles still serves all tools; only `resources/read` returns an actionable
"bundle missing" error.

## Configuration

| Setting | Env var | Default |
|---|---|---|
| Enable the surface | `OSA_MCP__ENABLED` | `true` |
| Widget bundle directory | `OSA_MCP__WIDGET_BUNDLE_DIR` | packaged bundles |

## Security posture

- Public and read-only, exactly like `/data/`; tool dispatch runs as an
  anonymous principal against `public()` handlers.
- Widgets render in the host's mandatory sandboxed, different-origin iframe
  under a **default-deny CSP** (`_meta.ui.csp` grants no origins); all data
  arrives via host-proxied tool calls, auditable as JSON-RPC.
- DNS-rebinding protection is explicitly disabled at the transport (public
  virtual-hosted deployments validate `Host` at the ingress); the SDK default
  would be silently off anyway — we made the choice visible.

## Known limits (deliberate, MVP)

- **Charts aggregate client-side over one bounded page** (the `/data/` page
  clamp, default ≤ 1000 rows). The chart widget shows an explicit truncation
  notice beyond the cap. Server-side aggregation (`AggregateExpr`, GROUP BY)
  is a planned fast-follow.
- **`instructions` are a startup snapshot** of `SKILL.md`. Live grounding
  comes from `list_datasets` / `describe_dataset`; a node restart refreshes
  the snapshot.
- Bundled-HTML widgets only (the MCP Apps MVP content type); operator-authored
  custom widgets via `osa deploy` are future work
  (`docs/design/widget-system.md`).
