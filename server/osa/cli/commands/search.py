"""Search commands."""

import os
import re
import sys
import time
from collections.abc import Callable

import cyclopts
import httpx

from osa.cli.console import get_console
from osa.cli.models import RecordMetadata, SearchHit
from osa.cli.util import OSAPaths, write_search_cache

app = cyclopts.App(name="search", help="Search commands")


def extract_short_id(srn: str) -> str:
    """Extract short ID (first 6 chars of UUID) from SRN.

    SRN format: urn:osa:{domain}:{type}:{uuid}[@{version}]
    """
    match = re.search(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", srn)
    if match:
        return match.group(0)[:6]
    return srn[:6]


def get_server_url() -> str:
    """Get server URL from config or environment."""
    return os.environ.get("OSA_SERVER", "http://localhost:8000")


def with_retry[T](
    fn: Callable[[], T],
    retries: int = 3,
    exceptions: tuple[type[Exception], ...] = (Exception,),
) -> T:
    """Retry a function on transient errors with exponential backoff."""
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            return fn()
        except exceptions as e:
            last_error = e
            if attempt < retries:
                time.sleep(0.2 * (attempt + 1))
    raise last_error  # type: ignore[misc]


@app.default
def search(
    index: str,
    query: str,
    /,
    limit: int = 20,
) -> None:
    """Search an index by name.

    Args:
        index: Name of the index to search (e.g., 'geo', 'vector')
        query: Search query
        limit: Maximum number of results
    """
    console = get_console()
    server_url = get_server_url()
    url = f"{server_url}/api/v1/search/{index}"

    try:
        response = with_retry(
            lambda: httpx.get(url, params={"q": query, "limit": limit}),
            exceptions=(httpx.ReadError, httpx.ConnectError),
        )
        response.raise_for_status()
        data = response.json()

        total = data.get("total", 0)
        results = data.get("results", [])

        # Build typed SearchHit models
        hits: list[SearchHit] = []
        for hit in results:
            srn = hit.get("id", "")
            hits.append(
                SearchHit(
                    srn=srn,
                    short_id=extract_short_id(srn),
                    score=hit.get("score", 0),
                    metadata=RecordMetadata.model_validate(hit.get("metadata", {})),
                )
            )

        # Display results
        console.search_results(hits, total, index)

        # Save to cache for `osa show <number>` lookup
        if hits:
            paths = OSAPaths()
            write_search_cache(paths.search_cache_file, index=index, query=query, results=hits)
            console.search_hint()

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


@app.command
def list_indexes() -> None:
    """List available search indexes."""
    console = get_console()
    server_url = get_server_url()
    url = f"{server_url}/api/v1/search/"

    try:
        response = with_retry(
            lambda: httpx.get(url),
            exceptions=(httpx.ReadError, httpx.ConnectError),
        )
        response.raise_for_status()
        data = response.json()

        indexes = data.get("indexes", [])
        if indexes:
            console.print("[bold]Available indexes:[/bold]")
            for idx in indexes:
                console.print(f"  [cyan]{idx}[/cyan]")
        else:
            console.warning("No indexes configured")

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
        console.error("Connection lost while reading response")
        sys.exit(1)
