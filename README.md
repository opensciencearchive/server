<p align="center">
  <img src="https://opensciencearchive.org/osa_logo.svg" alt="OSA Logo" width="120" />
</p>

<h1 align="center">Open Science Archive</h1>

<p align="center">
  <strong>Natural language search for scientific datasets</strong>
</p>

<p align="center">
  <a href="#quick-start">Quick Start</a> •
  <a href="#features">Features</a> •
  <a href="#usage">Usage</a> •
  <a href="#development">Development</a>
</p>

---

OSA is a local-first server for discovering scientific data. It ingests metadata from public repositories like [NCBI GEO](https://www.ncbi.nlm.nih.gov/geo/), indexes it with vector embeddings, and provides natural language search via CLI.

## Quick Start

**Requirements:** Python 3.13+, [uv](https://docs.astral.sh/uv/)

```bash
# Clone and install
git clone https://github.com/opendatabank/osa.git
cd osa
uv sync

# Initialize OSA (creates config at ~/.config/osa/)
uv run osa init --email your@email.com

# Start the server
uv run osa server start

# Search GEO datasets
uv run osa search vector "single cell RNA-seq alzheimer's disease"
```

## Features

- **Natural language search** — Find datasets by describing what you're looking for
- **GEO integration** — Automatic ingestion from NCBI Gene Expression Omnibus
- **Local-first** — Runs entirely on your machine, no cloud dependencies
- **Vector search** — Semantic similarity powered by sentence-transformers

## Usage

### CLI Commands

```bash
# Search for datasets
osa search vector "breast cancer tumor microenvironment"

# View details of a result (by number from search)
osa show 1

# Check system stats
osa stats

# View server status
osa server status

# View logs
osa server logs --follow
```

### Configuration

OSA stores configuration in `~/.config/osa/config.yaml`. Data is stored in `~/.local/share/osa/`.

## Development

```bash
# Install dev dependencies
uv sync --group dev

# Run tests
uv run pytest

# Type checking
uv run pyright

# Linting
uv run ruff check
```

## License

MIT
