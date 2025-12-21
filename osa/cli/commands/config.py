"""Config management commands."""

import json
import sys
from pathlib import Path

import cyclopts

app = cyclopts.App(name="config", help="Manage OSA configuration")

TEMPLATE = """\
# OSA Configuration
# See https://docs.osa.dev/config for full reference

server:
  name: "My OSA Node"
  domain: "localhost"

# Database (defaults to SQLite at ~/.osa/osa.db)
# database:
#   url: "sqlite+aiosqlite:///~/.osa/osa.db"
#   auto_migrate: true

# Logging (defaults to DEBUG, logs to ~/.osa/logs/server.log when daemonized)
# logging:
#   level: "INFO"

ingestors:
  geo:
    ingestor: geo
    config:
      email: your-email@example.com  # Required by NCBI - please update!
      # api_key: null  # Optional: get from NCBI for higher rate limits
    initial_run:  # Ingest a few records on first startup
      enabled: true
      limit: 5
    # schedule:  # Uncomment to enable periodic ingestion
    #   cron: "0 * * * *"  # Every hour at :00
    #   limit: 100

indexes:
  vector:
    backend: vector
    config:
      persist_dir: ~/.osa/data/vectors
      embedding:
        model: all-MiniLM-L6-v2
        fields: [title, summary]
"""


DEFAULT_CONFIG_NAME = "osa.yaml"


@app.command
def init(path: Path = Path(DEFAULT_CONFIG_NAME)) -> None:
    """Create a new config file from template.

    Args:
        path: Path for the config file. Defaults to ./osa.yaml
    """
    if path.is_dir():
        print(f"Error: {path} is a directory, not a file path", file=sys.stderr)
        sys.exit(1)

    if path.exists():
        print(f"Error: {path} already exists (refusing to overwrite)", file=sys.stderr)
        sys.exit(1)

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(TEMPLATE)
    print(f"Created config at {path}")
    print("Edit the file to configure your OSA node, then run:")
    if path.name == DEFAULT_CONFIG_NAME:
        print("  osa server start")
    else:
        print(f"  osa server start --config {path}")


@app.command
def validate(path: Path = Path(DEFAULT_CONFIG_NAME)) -> None:
    """Validate a config file.

    Args:
        path: Path to the config file. Defaults to ./osa.yaml
    """
    import yaml

    from osa.config import Config

    if not path.exists():
        print(f"Error: {path} not found", file=sys.stderr)
        sys.exit(1)

    try:
        data = yaml.safe_load(path.read_text())
        Config.model_validate(data)
        print(f"✓ {path} is valid")
    except Exception as e:
        print(f"✗ {path} is invalid: {e}", file=sys.stderr)
        sys.exit(1)


@app.command
def show() -> None:
    """Show current effective config."""
    from osa.config import Config

    config = Config()
    print(json.dumps(config.model_dump(mode="json"), indent=2, default=str))
