"""Configuration management commands."""

import sys
from pathlib import Path
from typing import Literal

import cyclopts

from osa.cli.console import get_console
from osa.cli.util import OSAPaths

app = cyclopts.App(name="config", help="Configuration management")

# Available templates
Template = Literal["geo", "minimal"]
TEMPLATES: list[Template] = ["geo", "minimal"]

# Default output file in current working directory
DEFAULT_OUTPUT = Path("osa.yaml")


def _get_template_content(template: Template, paths: OSAPaths) -> str:
    """Get template content with paths substituted."""
    if template == "geo":
        return GEO_TEMPLATE.format(vectors_dir=paths.vectors_dir)
    else:
        return MINIMAL_TEMPLATE.format(vectors_dir=paths.vectors_dir)


GEO_TEMPLATE = """\
# OSA Configuration - GEO Template

server:
  name: "My OSA Node"
  domain: "localhost"

# Database (SQLite by default, stored in $OSA_DATA_DIR or ~/.local/share/osa/)
# database:
#   auto_migrate: true

# Logging
# logging:
#   level: "INFO"

# GEO Ingestor - pulls from NCBI Gene Expression Omnibus via Entrez API
ingestors:
  - ingestor: geo-entrez
    config:
      record_type: gse  # gse (~250k all) or gds (~5k curated)
      email: your@email.com  # Required by NCBI - please update this
      # api_key: null  # Optional: NCBI API key for higher rate limits (https://account.ncbi.nlm.nih.gov/settings/)
    initial_run:
      enabled: true
      limit: 50
    # schedule:
    #   cron: "0 * * * *"  # Hourly
    #   limit: 100

# Vector search index
indexes:
  - name: vector
    backend: vector
    config:
      persist_dir: {vectors_dir}
      embedding:
        model: all-MiniLM-L6-v2
        fields: [title, summary, organism, platform, entry_type]
"""

MINIMAL_TEMPLATE = """\
# OSA Configuration

server:
  name: "My OSA Node"
  domain: "localhost"

# Database (SQLite by default, stored in $OSA_DATA_DIR or ~/.local/share/osa/)
# database:
#   auto_migrate: true

# Logging
# logging:
#   level: "INFO"

# Add your ingestors here:
# ingestors:
#   - ingestor: geo-entrez
#     config:
#       email: your@email.com

# Add your indexes here:
# indexes:
#   - name: my-index
#     backend: vector
#     config:
#       persist_dir: {vectors_dir}
"""


@app.command
def init(
    template: Template | None = None,
    /,
    output: Path | None = None,
    stdout: bool = False,
    force: bool = False,
) -> None:
    """Generate a configuration file from a template.

    Creates an osa.yaml configuration file in the current directory (or specified path).
    Edit this file to customize your OSA instance, then run 'osa local start'.

    Args:
        template: Template to use (geo, minimal).
        output: Output path. Defaults to ./osa.yaml.
        stdout: Print to stdout instead of writing to file.
        force: Overwrite existing file.
    """
    console = get_console()
    paths = OSAPaths()

    # If no template specified, show available options
    if template is None:
        console.print("[bold]Available templates:[/bold]\n")
        console.print("  [cyan]geo[/cyan]      NCBI GEO integration with vector search")
        console.print("  [cyan]minimal[/cyan]  Blank configuration to customize")
        console.print()
        console.print("Usage: [bold]osa config init <template>[/bold]")
        console.print()
        console.print("Examples:")
        console.print("  [dim]osa config init geo[/dim]              # Write to ./osa.yaml")
        console.print("  [dim]osa config init geo --stdout[/dim]     # Print to stdout")
        console.print("  [dim]osa config init geo -o config.yaml[/dim]  # Custom output path")
        sys.exit(0)

    # At this point template is guaranteed to be set (sys.exit never returns)
    assert template is not None

    # Generate config content
    config_content = _get_template_content(template, paths)

    # Output to stdout if requested
    if stdout:
        print(config_content)
        return

    # Determine output path
    output_path = output or DEFAULT_OUTPUT

    # Check if file exists
    if output_path.exists() and not force:
        console.warning(f"File already exists: {output_path}")
        console.info("Use --force to overwrite")
        sys.exit(1)

    # Write config file
    output_path.write_text(config_content)

    # Show results
    console.success(f"Created {output_path}")
    console.print()
    console.print(f"Edit your config: [bold]{output_path}[/bold]")
    console.print("Then run: [bold]osa local start[/bold]")


@app.command
def path() -> None:
    """Show configuration file locations.

    Displays where OSA looks for configuration files.
    """
    console = get_console()
    paths = OSAPaths()

    console.print("[bold]Config resolution order:[/bold]\n")
    console.print("  1. [cyan]./osa.yaml[/cyan] (current directory)")
    console.print(f"  2. [cyan]{paths.config_file}[/cyan] (OSA_DATA_DIR or XDG)")
    console.print()

    # Show which ones exist
    local_config = Path("osa.yaml")
    if local_config.exists():
        console.print("  [green]Found:[/green] ./osa.yaml")
    if paths.config_file.exists():
        console.print(f"  [green]Found:[/green] {paths.config_file}")
    if not local_config.exists() and not paths.config_file.exists():
        console.print("  [dim]No config files found (server will use defaults)[/dim]")
