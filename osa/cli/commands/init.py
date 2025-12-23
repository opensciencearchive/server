"""Initialize OSA configuration and directories."""

import sys

import cyclopts

from osa.cli.console import get_console
from osa.cli.util import OSAPaths

app = cyclopts.App(name="init", help="Initialize OSA")

CONFIG_TEMPLATE = """\
# OSA Configuration
# Documentation: https://docs.osa.dev/config

server:
  name: "My OSA Node"
  domain: "localhost"

# Database (SQLite by default)
# database:
#   url: "sqlite+aiosqlite:///{data_dir}/osa.db"
#   auto_migrate: true

# Logging
# logging:
#   level: "INFO"

ingestors:
  geo:
    ingestor: geo
    config:
      email: {email}
      # api_key: null  # Optional: NCBI API key for higher rate limits
    initial_run:
      enabled: true
      limit: 50
    # schedule:
    #   cron: "0 * * * *"  # Hourly
    #   limit: 100

indexes:
  vector:
    backend: vector
    config:
      persist_dir: {vectors_dir}
      embedding:
        model: all-MiniLM-L6-v2
        fields: [title, summary, organism, platform, entry_type]
"""


@app.default
def init(
    email: str | None = None,
    force: bool = False,
) -> None:
    """Initialize OSA configuration and directories.

    Creates the config file and directory structure needed to run OSA.

    Args:
        email: Email for NCBI API access (required for GEO ingestion).
        force: Overwrite existing configuration.
    """
    console = get_console()
    paths = OSAPaths()

    # Check if already initialized
    if paths.is_initialized() and not force:
        console.warning("OSA is already initialized")
        console.print(f"  [dim]Config:[/dim] {paths.config_file}")
        console.info("Use --force to reinitialize")
        sys.exit(0)

    # Show what we're going to create
    console.print("[bold]OSA Setup[/bold]\n")
    console.print("This will create:")
    console.print(f"  [cyan]{paths.config_file}[/cyan]  [dim](configuration)[/dim]")
    console.print(f"  [cyan]{paths.data_dir}/[/cyan]  [dim](database, indexes)[/dim]")
    console.print(f"  [cyan]{paths.state_dir}/[/cyan]  [dim](logs, runtime state)[/dim]")
    console.print(f"  [cyan]{paths.cache_dir}/[/cyan]  [dim](cache)[/dim]")
    console.print()

    # Get email if not provided
    if email is None:
        console.print("[dim]NCBI requires an email address for GEO API access.[/dim]")
        try:
            email = input("Email: ").strip()
        except (KeyboardInterrupt, EOFError):
            console.print()
            console.error("Setup cancelled")
            sys.exit(1)

        if not email:
            console.error("Email is required for GEO ingestion")
            sys.exit(1)

    console.print()

    # Create directories
    paths.ensure_directories()

    # Write config file
    config_content = CONFIG_TEMPLATE.format(
        email=email,
        data_dir=paths.data_dir,
        vectors_dir=paths.vectors_dir,
    )
    paths.config_file.write_text(config_content)

    console.success(f"Created {paths.config_file}")
    console.success(f"Created {paths.data_dir}/")
    console.success(f"Created {paths.state_dir}/")
    console.success(f"Created {paths.cache_dir}/")
    console.print()
    console.print("Run [bold]osa server start[/bold] to start the server.")
