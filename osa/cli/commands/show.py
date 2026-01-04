"""Show command for viewing record details."""

import sys

import cyclopts

from osa.cli.console import get_console
from osa.cli.models import SearchHit
from osa.cli.util import OSAPaths

app = cyclopts.App(name="show", help="Show record details from last search")


@app.default
def show(ref: str, /) -> None:
    """Show details for a record from the last search.

    Args:
        ref: Result number (1, 2, ...) or short ID (e.g., 'ede67f')
    """
    console = get_console()
    paths = OSAPaths()
    cache = paths.read_search_cache()

    if cache is None:
        console.error(
            "No search results cached",
            hint='Run a search first: osa search vector "your query"',
        )
        sys.exit(1)
        return  # Unreachable, but helps type checker

    # Try to interpret ref as a number first
    result: SearchHit | None = None
    if ref.isdigit():
        idx = int(ref) - 1  # Convert to 0-based index
        if 0 <= idx < len(cache.results):
            result = cache.results[idx]
        else:
            console.error(
                f"Invalid result number: {ref}",
                hint=f"Last search had {len(cache.results)} results",
            )
            sys.exit(1)
    else:
        # Try to match by short ID prefix
        ref_lower = ref.lower()
        matches = [r for r in cache.results if r.short_id.startswith(ref_lower)]
        if len(matches) == 1:
            result = matches[0]
        elif len(matches) > 1:
            console.error(f"Ambiguous short ID '{ref}'")
            console.print("[dim]Matches:[/dim]")
            for m in matches:
                console.print(f"  [cyan]{m.short_id}[/cyan]  {m.metadata.title[:50]}")
            sys.exit(1)
        else:
            console.error(
                f"No match for '{ref}' in last search results",
                hint=f"Last search: '{cache.query}' ({len(cache.results)} results)",
            )
            sys.exit(1)

    # Display the result
    console.record_detail(result)
