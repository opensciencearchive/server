# @opensciencearchive/osa-widgets

MCP Apps widget library for the OSA platform (issue #162). Five React widgets
— dataset overview, records table, chart, record detail, and filter panel —
each built into a **single self-contained HTML file** that the OSA MCP server
serves as a `ui://osa/*` resource. AI hosts (Claude, etc.) render the file in
a sandboxed iframe with a default-deny CSP: no external network access, so
all JS/CSS is inlined at build time.

## Build

```sh
pnpm install
pnpm build     # → dist/dataset-overview.html, table.html, chart.html,
               #   record.html, filter-panel.html
pnpm test      # vitest
pnpm lint      # tsc --noEmit
```

The build (`build.mjs`) bundles each `src/widgets/<name>/index.tsx` with
esbuild (iife, minified, es2020), inlines `src/styles/base.css`, escapes
`</script>` sequences, and fails if any output references an http(s) URL.

## How bundles are served

The server reads the built files from `osa/application/api/mcp/bundles/` and
exposes them as MCP resources; copy or wire `dist/*.html` there as part of
the server build.

## The protocol seam

Widgets talk to the host over postMessage JSON-RPC via a hand-rolled shim in
`src/protocol/` (`jsonrpc.ts` framing, `host.ts` message names + payload
probing). **MCP Apps is a young spec and hosts differ** — e.g. where the
initial render data lives (initialize response vs. a `ui/render-data`
notification). All such tolerance is deliberately concentrated in those two
modules; when the spec stabilizes, update them and nothing else.

Data flows in from the host on connect; widgets call server tools back
through the host (`tools/call`: `fetch_page`, `sample_values`) and inform
the model of user actions with short `ui/update-model-context` notifications.
