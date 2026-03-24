<p align="center">
  <img src="https://opensciencearchive.org/osa_logo.svg" alt="OSA Logo" width="120" />
</p>

<h1 align="center">Open Science Archive</h1>

<p align="center">
  <strong>A domain-agnostic archive for AI-ready scientific data</strong>
  <br /><br />
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

### Canonical Write Path

```
Deposition  ─→  Validation  ─→  Curation  ─→  Record  ─→  Search & Export
   draft          OCI hooks      approve/       immutable     indexed,
   metadata       structured     reject         versioned     exportable
   + files        checks                        published
```

## Status

OSA is in **early development**. The core write path (deposition through record publication) is functional. Search, export, and federation are in progress. The web UI is minimal.

## Project Structure

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

## License

Apache 2.0
