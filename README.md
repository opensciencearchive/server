<p align="center">
  <img src="https://opensciencearchive.org/osa_logo.svg" alt="OSA Logo" width="120" />
</p>

<h1 align="center">Open Science Archive</h1>

<p align="center">
  <strong>A domain-agnostic archive for AI-ready scientific data</strong>
</p>

<p align="center">
  <a href="#quick-start">Quick Start</a> •
  <a href="#templates">Templates</a> •
  <a href="#usage">Usage</a> •
  <a href="#development">Development</a>
</p>

---

OSA makes it easy to stand up [PDB](https://www.rcsb.org/)-level data infrastructure for any scientific domain — validated, searchable, and AI-ready.

## Quick Start

**Requirements:** Python 3.13+, [uv](https://docs.astral.sh/uv/)

```bash
# Clone and install
git clone https://github.com/opendatabank/osa.git
cd osa
uv sync
source .venv/bin/activate

# Initialize with the GEO template
osa init geo

# Start the server
osa server start

# Search datasets using natural language
osa search vector "single cell RNA-seq alzheimer's disease"
```

## Templates

OSA ships with pre-configured templates for different domains:

| Template | Description |
|----------|-------------|
| **geo** | [NCBI GEO](https://www.ncbi.nlm.nih.gov/geo/) integration with vector search. Natural language search over gene expression datasets. |
| **minimal** | Blank configuration for building your own archive. |

```bash
# See available templates
osa init

# Initialize with a specific template
osa init geo
osa init minimal
```

More templates coming soon. Contributions welcome.

## Usage

### CLI Commands

```bash
# Search for datasets
osa search vector "breast cancer tumor microenvironment"

# View details of a result (by number from search)
osa show 1

# Check system stats
osa stats

# Server management
osa server status
osa server logs --follow
osa server stop
```

### Configuration

Configuration lives in `~/.config/osa/config.yaml`. Data is stored in `~/.local/share/osa/`.

## Development

```bash
# Install dev dependencies
uv sync --group dev

# Run tests
uv run pytest

# Type checking & linting
uv run pyright
uv run ruff check
```

## License

MIT
