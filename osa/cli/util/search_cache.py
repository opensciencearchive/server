"""Search cache file I/O utilities.

Handles reading and writing the search results cache file that enables
numbered result lookup in CLI (e.g., `osa show 1`).
"""

from datetime import UTC, datetime
from pathlib import Path

from pydantic import ValidationError

from osa.cli.models import SearchCache, SearchHit


def read_search_cache(cache_file: Path) -> SearchCache | None:
    """Read cached search results.

    Args:
        cache_file: Path to the cache file (e.g., last_search.json).

    Returns:
        SearchCache if file exists and is valid, None otherwise.
    """
    if not cache_file.exists():
        return None
    try:
        data = cache_file.read_text()
        return SearchCache.model_validate_json(data)
    except (ValidationError, OSError):
        return None


def write_search_cache(
    cache_file: Path,
    index: str,
    query: str,
    results: list[SearchHit],
) -> SearchCache:
    """Write search results to cache for numbered lookup.

    Creates parent directories if they don't exist.

    Args:
        cache_file: Path to the cache file.
        index: Name of the index that was searched.
        query: The search query string.
        results: List of search result hits.

    Returns:
        The created SearchCache.
    """
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    cache = SearchCache(
        index=index,
        query=query,
        searched_at=datetime.now(UTC).isoformat(),
        results=results,
    )
    cache_file.write_text(cache.model_dump_json(indent=2))
    return cache
