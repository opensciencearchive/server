"""Stats command - show system statistics."""

import sys

import cyclopts
import httpx

from osa.cli.commands.search import get_server_url, with_retry
from osa.cli.console import get_console

app = cyclopts.App(name="stats", help="Show system statistics")


@app.default
def stats() -> None:
    """Show system statistics (records, indexes)."""
    console = get_console()
    server_url = get_server_url()
    url = f"{server_url}/api/v1/stats"

    try:
        response = with_retry(
            lambda: httpx.get(url),
            exceptions=(httpx.ReadError, httpx.ConnectError),
        )
        response.raise_for_status()
        data = response.json()

        # Records
        record_count = data.get("records", 0)
        console.print(f"[bold]Records:[/bold] {record_count:,}")
        console.print()

        # Indexes
        indexes = data.get("indexes", [])
        if indexes:
            console.print("[bold]Indexes:[/bold]")
            for idx in indexes:
                name = idx.get("name", "unknown")
                count = idx.get("count", 0)
                healthy = idx.get("healthy", False)
                status = "[green]✓[/green]" if healthy else "[red]✗[/red]"
                console.print(f"  {status} [cyan]{name}[/cyan]: {count:,} documents")
        else:
            console.print("[dim]No indexes configured[/dim]")

    except httpx.ConnectError:
        console.error(
            f"Could not connect to server at {server_url}",
            hint="Is the server running? Start it with: osa server start",
        )
        sys.exit(1)
    except httpx.HTTPStatusError as e:
        console.error(f"Server error: {e.response.status_code} - {e.response.text}")
        sys.exit(1)
    except httpx.ReadError:
        console.error(
            "Connection lost while reading response",
            hint="The server may have crashed. Check: osa server logs",
        )
        sys.exit(1)
