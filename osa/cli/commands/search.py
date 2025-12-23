"""Search commands."""

import os
import re
import sys
import time
from collections.abc import Callable

import cyclopts
import httpx

from osa.cli.util import OSAPaths

app = cyclopts.App(name="search", help="Search commands")


def extract_short_id(srn: str) -> str:
    """Extract short ID (first 6 chars of UUID) from SRN.

    SRN format: urn:osa:{domain}:{type}:{uuid}[@{version}]
    """
    # Match UUID pattern in SRN
    match = re.search(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", srn)
    if match:
        return match.group(0)[:6]
    return srn[:6]  # Fallback


def get_server_url() -> str:
    """Get server URL from config or environment."""
    return os.environ.get("OSA_SERVER", "http://localhost:8000")


def with_retry[T](
    fn: Callable[[], T],
    retries: int = 3,
    exceptions: tuple[type[Exception], ...] = (Exception,),
) -> T:
    """Retry a function on transient errors with exponential backoff.

    Args:
        fn: Zero-argument callable to retry.
        retries: Max retry attempts (total attempts = retries + 1).
        exceptions: Exception types to catch and retry on.

    Returns:
        Result of fn() on success.

    Raises:
        The last exception if all retries fail.
    """
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            return fn()
        except exceptions as e:
            last_error = e
            if attempt < retries:
                time.sleep(0.2 * (attempt + 1))  # Backoff: 0.2, 0.4, 0.6s
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

        print(f"Found {total} results in index '{index}':\n")

        # Build cache entries while displaying results
        cache_results: list[dict] = []

        for i, hit in enumerate(results, 1):
            srn = hit.get("id", "")
            short_id = extract_short_id(srn)
            score = hit.get("score", 0)
            metadata = hit.get("metadata", {})
            title = metadata.get("title", "Untitled")
            organism = metadata.get("organism", "Unknown")
            sample_count = metadata.get("sample_count", "?")
            pub_date = metadata.get("pub_date", "Unknown")

            print(f"[{i}] {short_id}  {title}")
            print(f"    {organism} | {sample_count} samples | {pub_date} (score: {score:.2f})")
            print()

            # Add to cache
            cache_results.append({
                "srn": srn,
                "short_id": short_id,
                "metadata": metadata,
            })

        # Save to cache for `osa show <number>` lookup
        if cache_results:
            paths = OSAPaths()
            paths.write_search_cache(index=index, query=query, results=cache_results)
            print("Use 'osa show <number>' to view details (e.g., 'osa show 1')")

    except httpx.ConnectError:
        print(f"Error: Could not connect to server at {server_url}", file=sys.stderr)
        print("Is the server running? Start it with: osa server start", file=sys.stderr)
        sys.exit(1)
    except httpx.HTTPStatusError as e:
        print(f"Error: {e.response.status_code} - {e.response.text}", file=sys.stderr)
        sys.exit(1)
    except httpx.ReadError:
        print("Error: Connection lost while reading response", file=sys.stderr)
        sys.exit(1)


@app.command
def list_indexes() -> None:
    """List available search indexes."""
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
            print("Available indexes:")
            for idx in indexes:
                print(f"  - {idx}")
        else:
            print("No indexes configured")

    except httpx.ConnectError:
        print(f"Error: Could not connect to server at {server_url}", file=sys.stderr)
        sys.exit(1)
    except httpx.HTTPStatusError as e:
        print(f"Error: {e.response.status_code} - {e.response.text}", file=sys.stderr)
        sys.exit(1)
    except httpx.ReadError:
        print("Error: Connection lost while reading response", file=sys.stderr)
        sys.exit(1)
