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

# NOTE: update the config.yaml with your NCBI API key and increase the record ingestion limit

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

#### GEO Template Setup

After running `osa init geo`, edit your config file to:

1. **Add your NCBI API key** (recommended for faster ingestion):
   - Get a free API key at https://account.ncbi.nlm.nih.gov/settings/
   - Without an API key, NCBI limits you to 3 requests/second
   - With an API key, you get 10 requests/second

2. **Increase the initial ingestion limit** for a more complete dataset:

```yaml
ingestors:
  - ingestor: geo-entrez
    config:
      email: your@email.com
      api_key: your_ncbi_api_key_here  # Optional but recommended
    initial_run:
      enabled: true
      limit: 10000  # Increase from default 50 for fuller dataset
```

The GEO template uses the full GSE dataset (~250,000 series). Initial ingestion of 10,000 records takes roughly 20-30 minutes with an API key.

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

Apache 2.0
