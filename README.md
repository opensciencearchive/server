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

<table>
<tr>
<td width="50%">

**Convention-driven submissions**
Conventions bundle a metadata schema, validators, and file requirements into a single submission target.

**Pluggable validation**
Validators are OCI containers with a filesystem I/O contract. No network by default. Domain experts define quality checks; OSA runs them.

</td>
<td width="50%">

**Structured Resource Names**
Globally unique, node-scoped identifiers with clear versioning.
`urn:osa:{domain}:{type}:{id}[@{version}]`

**Federation-ready**
Nodes identified by DNS domain. Records flow between nodes via import, fork, and mirror — preserving provenance.

</td>
</tr>
</table>

## Quickstart

You don't need to clone this repo to run an OSA archive. The [Python SDK (`osa-py`)](https://github.com/opensciencearchive/osa-py) ships the whole stack — Postgres, server, and a docker-socket-proxy, brought up with one command.

```bash
pip install osa-py
osa init my-archive
cd my-archive
osa start
```

`osa start` spins up the stack via Docker Compose and mints a SUPERADMIN dev token so the CLI is authenticated immediately.

Define a convention in Python (schema + validation hooks + ingester), then:

```bash
osa deploy             # build hook images, register the convention
osa ingestion start    # pull records via the ingester
```

The full SDK reference — schemas, hooks, ingesters, the `osa test` end-to-end harness, and the convention manifest — lives in the [`osa-py` README](https://github.com/opensciencearchive/osa-py).

## Hack on OSA

Working on the server itself:

```bash
git clone https://github.com/opensciencearchive/server.git
cd server
just dev    # Postgres + server + web with hot-reload
```

Run tests: `cd server && just test`. Lint + type check: `just lint`.

```
osa/
├── server/                  # Python backend (FastAPI)
│   ├── osa/
│   │   ├── domain/          # DDD bounded contexts
│   │   ├── application/     # API routes, DI wiring
│   │   └── infrastructure/  # Adapters (DB, K8s, S3)
│   ├── tests/               # Unit + integration tests
│   ├── migrations/          # Alembic migrations
│   └── sources/             # Data source plugins
├── web/                     # Next.js frontend
│   └── src/                 # React components, pages
└── deploy/                  # Docker Compose orchestration
```

## Canonical Write Path

```
Deposition  ─→  Validation  ─→  Curation  ─→  Record  ─→  Search & Export
   draft          OCI hooks      approve/       immutable     indexed,
   metadata       structured     reject         versioned     exportable
   + files        checks                        published
```

## Status

OSA is in **early development**. The local-dev story is in good shape — `osa start` brings up a fully-authenticated stack with no config. The core write path (deposition through record publication) and the query layer (filtered search over records and feature tables) are both functional. Export, federation, and the web UI are still in progress.

## Demos

[Protein Pocket Database](https://www.pockets.bio/)
[Semantic GEO Database](https://www.lingual.bio/)

## License

Apache 2.0
