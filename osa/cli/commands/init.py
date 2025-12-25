"""Initialize OSA configuration and directories."""

import sys
from typing import Literal

import cyclopts

from osa.cli.console import get_console
from osa.cli.util import OSAPaths

app = cyclopts.App(name="init", help="Initialize OSA with a template")

# Available templates
Template = Literal["geo", "minimal"]
TEMPLATES: list[Template] = ["geo", "minimal"]

GEO_TEMPLATE = """\
# OSA Configuration - GEO Template
# Documentation: https://docs.osa.dev/config

server:
  name: "My OSA Node"
  domain: "localhost"

# Database (SQLite by default, stored in ~/.local/share/osa/)
# database:
#   auto_migrate: true

# Logging
# logging:
#   level: "INFO"

# GEO Ingestor - pulls from NCBI Gene Expression Omnibus
ingestors:
  geo:
    ingestor: geo
    config:
      email: your@email.com  # Required by NCBI - please update this
      # api_key: null  # Optional: NCBI API key for higher rate limits
    initial_run:
      enabled: true
      limit: 50
    # schedule:
    #   cron: "0 * * * *"  # Hourly
    #   limit: 100

# Vector search index
indexes:
  vector:
    backend: vector
    config:
      persist_dir: {vectors_dir}
      embedding:
        model: all-MiniLM-L6-v2
        fields: [title, summary, organism, platform, entry_type]
"""

MINIMAL_TEMPLATE = """\
# OSA Configuration
# Documentation: https://docs.osa.dev/config

server:
  name: "My OSA Node"
  domain: "localhost"

# Database (SQLite by default, stored in ~/.local/share/osa/)
# database:
#   auto_migrate: true

# Logging
# logging:
#   level: "INFO"

# Add your ingestors here:
# ingestors:
#   my_ingestor:
#     ingestor: ...
#     config: ...

# Add your indexes here:
# indexes:
#   my_index:
#     backend: vector
#     config:
#       persist_dir: {vectors_dir}
"""


@app.default
def init(
    template: Template | None = None,
    /,
    force: bool = False,
) -> None:
    """Initialize OSA configuration and directories.

    Creates the config file and directory structure needed to run OSA.

    Args:
        template: Template to use (geo, minimal).
        force: Overwrite existing configuration.
    """
    console = get_console()
    paths = OSAPaths()

    # If no template specified, show available options
    if template is None:
        console.print("[bold]Available templates:[/bold]\n")
        console.print("  [cyan]geo[/cyan]      NCBI GEO integration with vector search")
        console.print("  [cyan]minimal[/cyan]  Blank configuration to customize")
        console.print()
        console.print("Usage: [bold]osa init <template>[/bold]")
        console.print()
        console.print("Example: [dim]osa init geo[/dim]")
        sys.exit(0)

    # Check if already initialized
    if paths.is_initialized() and not force:
        console.warning("OSA is already initialized")
        console.print(f"  [dim]Config:[/dim] {paths.config_file}")
        console.info("Use --force to reinitialize")
        sys.exit(0)

    # Generate config content
    if template == "geo":
        config_content = GEO_TEMPLATE.format(vectors_dir=paths.vectors_dir)
    else:  # minimal
        config_content = MINIMAL_TEMPLATE.format(vectors_dir=paths.vectors_dir)

    # Create directories
    paths.ensure_directories()

    # Write config file
    paths.config_file.write_text(config_content)

    # Show results
    console.success(f"Initialized OSA with '{template}' template")
    console.print()
    console.print(f"  [cyan]Config:[/cyan]  {paths.config_file}")
    console.print(f"  [cyan]Data:[/cyan]    {paths.data_dir}/")
    console.print(f"  [cyan]State:[/cyan]   {paths.state_dir}/")
    console.print(f"  [cyan]Cache:[/cyan]   {paths.cache_dir}/")
    console.print()
    console.print(f"Edit your config: [bold]{paths.config_file}[/bold]")
    console.print("Then run: [bold]osa server start[/bold]")
