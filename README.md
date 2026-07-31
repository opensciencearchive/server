<p align="center">
  <img src="https://opensciencearchive.org/osa_logo.svg" alt="OSA Logo" width="120" />
</p>

<h1 align="center">Open Science Archive</h1>

<p align="center">
  <strong>A domain-agnostic archive for AI-ready scientific data</strong>
  <br /><br />
  <a href="https://github.com/opensciencearchive/osa-py"><img src="https://img.shields.io/badge/Python%20SDK-osa--py-blue?style=flat-square" alt="Python SDK" /></a>
  <a href="https://github.com/opensciencearchive/server/issues"><img src="https://img.shields.io/github/issues/opensciencearchive/server?style=flat-square" alt="Issues" /></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue?style=flat-square" alt="License" /></a>
</p>

> **⚠️ Under active development** — OSA is pre-release software. APIs, data formats, and configuration will change without notice. Not yet suitable for production use or external contributions.

---

## What is OSA?

OSA is both an **open protocol** and its **reference implementation** for scientific data deposition, validation, publication, discovery, and export — standing up [PDB](https://www.rcsb.org/)-level data infrastructure for any scientific domain.

A lab describes its data once, as a **convention**: a typed metadata schema, an ingester that pulls from wherever the data lives today, and hooks that derive computed features. OSA turns that into a running archive with a validated write path, a queryable read surface, and documentation that both humans and LLM agents can navigate.

<table>
<tr>
<td width="50%">

**Conventions as code**
A schema, an ingester, and hooks — declared in Python, deployed with one command. No YAML archaeology, no bespoke ETL.

**Hooks as OCI containers**
Domain experts define quality checks and derived features; OSA runs them sandboxed, with no network by default. Every produced row is traceable to the exact image digest and config that made it.

</td>
<td width="50%">

**Agent-native read surface**
Every node serves a machine-readable catalog (`/data`), a generated agent skill sheet (`/SKILL.md`), and a Model Context Protocol endpoint (`/mcp`) — so any LLM host can query it without bespoke integration.

**Federation-ready**
Nodes are identified by DNS domain and resources by versioned, node-scoped names, so records can flow between nodes while preserving provenance.

</td>
</tr>
</table>

## Quickstart

You don't need to clone this repo to run an OSA archive. The [Python SDK (`osa-py`)](https://github.com/opensciencearchive/osa-py) ships the whole stack — Postgres, server, management dashboard, and a docker-socket-proxy for hook execution, brought up with one command.

```bash
pip install osa-py
osa init my-archive
cd my-archive
osa start
```

`osa start` brings the stack up via Docker Compose and mints a SUPERADMIN dev token, so the CLI is authenticated immediately — there is no login step.

| | |
|---|---|
| API | `http://localhost:8000` |
| Dashboard | `http://localhost:8081` |

```bash
osa dashboard    # opens the dashboard in your browser, already signed in
```

`osa dashboard` mints a short-lived handoff proof from the project's `SESSION_SECRET` and hands it to the dashboard, which exchanges it for a session cookie. You never type a password, and no archive token reaches the browser — the dashboard proxies reads server-side over an allowlisted, GET-only path.

Run `osa start --no-ui` to start the API alone, without the dashboard.

## Define a convention

A convention is a Python package that declares a schema, its hooks, and an ingester, then registers itself through an `osa.conventions` entry point. In outline:

```python
from osa import Example, Field, Record, Schema, convention, hook

class PDBStructure(Schema):
    __schema_id__ = "pdb-structure"

    pdb_id: str = Field(description="RCSB PDB accession code.", examples=["1ABC"])
    method: str = Field(description="Experimental method used to solve the structure.")
    resolution: float | None = Field(default=None, unit="Å")

@hook
def find_pockets(record: Record[PDBStructure]) -> list[Pocket]:
    """Derive one feature row per detected binding pocket. Runs in its own container."""
    ...

convention(
    title="Protein Structures",
    description="Protein structures from the PDB, with pocket detection.",
    version="1.0.0",
    schema=PDBStructure,
    hooks=[find_pockets],
    ingester=PDBIngester,
    files={"accepted_types": [".cif", ".pdb"], "max_count": 5},
    purpose="What this dataset covers and what questions it answers.",
    examples=[Example(question=..., query=..., interpretation=...)],
)
```

```toml
# pyproject.toml
[project.entry-points."osa.conventions"]
pockets = "mypkg.convention"
```

Documentation is **mandatory**, not optional: a convention must supply a `purpose`, at least one worked `Example`, and three or more distinct trigger questions. Under-documented deploys are rejected with a 422 naming each gap, and there is no bypass flag. This is what makes the generated `/SKILL.md` and per-schema reference docs useful to an agent rather than a schema dump.

Then deploy and ingest:

```bash
osa deploy                               # build hook + ingester images, register the convention
osa ingestion start --convention pockets # pull from upstream and emit records
osa logs server -f                       # watch ingestion, validation, and hooks run
```

`osa test` runs a convention end-to-end — the ingester, then every hook — without touching the archive. The full SDK reference lives in the [`osa-py` README](https://github.com/opensciencearchive/osa-py).

## The read surface

Once records are published, the node serves them from the unified `/data/` surface — a catalog, per-schema manifests with live row counts, single-record fetch, filtered queries, and streaming CSV / gzipped-CSV dumps of both records and feature tables.

```bash
curl http://localhost:8000/                                             # node identity + published schemas
curl http://localhost:8000/api/v1/data                                  # catalog: schemas + table resources
curl http://localhost:8000/api/v1/data/pdb-structure                    # manifest: fields, tables, row counts
curl 'http://localhost:8000/api/v1/data/pdb-structure/records?limit=3'  # records as JSON
curl http://localhost:8000/api/v1/data/pdb-structure/pocket.csv         # a feature table as CSV
```

Filtered queries POST a structured filter expression, which pushes down to SQL:

```bash
curl -X POST http://localhost:8000/api/v1/data/pdb-structure/pocket \
  -H 'Content-Type: application/json' \
  -d '{"filter": {"kind": "predicate",
                  "field": "features.pocket.score",
                  "op": "gte", "value": 0.8},
      "limit": 5}'
```

### Agents

Every node also serves `/SKILL.md` — a skill sheet generated from the live catalog, listing each dataset, its row counts, and its access patterns — and a [Model Context Protocol](https://modelcontextprotocol.io) endpoint at `/mcp` with the MCP Apps extension. Add that URL as a connector in any MCP-Apps host (Claude, ChatGPT, Goose, VS Code) and the assistant is grounded in the node's real datasets from the moment it connects, rendering sortable tables, charts, and filter panels inline. The node never calls a model itself; the connecting assistant does the orchestrating. See [`docs/mcp-apps.md`](docs/mcp-apps.md).

## The dashboard

`osa dashboard` opens the operator view of a single node. Every panel reads live data from the archive API:

| Page | What it shows |
|---|---|
| **Overview** | Record and feature-row counts, with a per-schema breakdown |
| **Records / Features** | Browse published records and the derived feature tables |
| **Agents** | The node's rendered `SKILL.md` and its MCP connector details |
| **Hooks** | Registered hooks and their live release versions |
| **Ingesters / Ingestions** | Configured ingesters, and ingestion runs with live progress |
| **Observability** | Component health |
| **Authentication** | Configured auth provider and admin list |

## Canonical write path

```
Deposition  ─→  Validation  ─→  Curation  ─→  Record  ─→  /data
   draft          OCI hooks      approve/     immutable    catalog, manifests,
   metadata       structured     reject       versioned    filtered queries,
   + files        checks                      published    CSV dumps, /mcp
```

## Hack on OSA

Working on the server or dashboard itself:

```bash
git clone https://github.com/opensciencearchive/server.git
cd server
just dev    # Postgres + server + dashboard + web, with hot-reload
```

Server tests: `cd server && just test`. Lint and type check: `just lint`.
Dashboard: `just dashboard dev`, `just dashboard test`.

```
.
├── server/                  # Python backend (FastAPI)
│   ├── osa/
│   │   ├── domain/          # DDD bounded contexts
│   │   ├── application/     # API routes, DI wiring
│   │   └── infrastructure/  # Adapters (DB, K8s, S3)
│   ├── tests/               # Unit + integration tests
│   ├── migrations/          # Alembic migrations
│   └── sources/             # Data source plugins
├── apps/
│   └── dashboard/           # Next.js management dashboard (operator UI)
├── packages/
│   └── osa-widgets/         # MCP Apps interactive UI bundles
├── web/                     # Next.js public archive site (deposit/search/record)
├── deploy/                  # Docker Compose orchestration
└── docs/                    # Architecture and design notes
```

> The `web/` public site is currently out of date. It builds under `just dev` here, but is **not** started by `osa start` — it sits behind a `web` Compose profile there, so the default self-host stack is server + dashboard.

## Status

The local-dev story is in good shape: `osa start` brings up a fully-authenticated stack with no config, and `osa dashboard` gets you a live operator view in one command. The write path (ingestion through record publication with containerised hooks and per-row provenance) and the read path (the unified `/data/` surface, generated agent docs, and the MCP endpoint) are both functional and running in the wild.

Human curation is auto-approve for v1. Federation, usage analytics, and the public archive site are still in progress.

## Demos

- [Protein Pocket Database](https://www.pockets.bio/)
- [Semantic GEO Database](https://www.lingual.bio/)

## License

Apache 2.0
