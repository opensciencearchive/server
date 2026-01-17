"""Events command - view the event log."""

import sys

import cyclopts
import httpx

from osa.cli.commands.search import get_server_url, with_retry
from osa.cli.console import get_console, relative_time

app = cyclopts.App(name="events", help="View the event log")


@app.default
def events(
    limit: int = 20,
    types: list[str] | None = None,
) -> None:
    """Show recent events from the event log (newest first).

    Args:
        limit: Number of events to show.
        types: Filter by event types (e.g., RecordPublished).
    """
    console = get_console()
    server_url = get_server_url()
    url = f"{server_url}/api/v1/events"

    params: dict[str, str | int | list[str]] = {"limit": limit, "order": "desc"}
    if types:
        params["types"] = types

    try:
        response = with_retry(
            lambda: httpx.get(url, params=params),
            exceptions=(httpx.ReadError, httpx.ConnectError),
        )
        response.raise_for_status()
        data = response.json()

        events_list = data.get("events", [])
        has_more = data.get("has_more", False)

        if not events_list:
            console.info("No events found")
            return

        more_indicator = " [dim](more available)[/dim]" if has_more else ""
        console.print(f"[bold]Events[/bold] [dim]({len(events_list)})[/dim]{more_indicator}\n")

        for event in events_list:
            event_type = event.get("type", "Unknown")
            created_at = event.get("created_at", "")

            # Format time
            time_str = relative_time(created_at) if created_at else ""

            console.print(f"[cyan]{event_type}[/cyan]  [dim]{time_str}[/dim]")

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
