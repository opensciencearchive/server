"""GEO Entrez source - uses NCBI E-utilities API."""

import asyncio
import logging
import random
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any
from xml.etree import ElementTree

import httpx

from osa.sdk.source.record import UpstreamRecord
from osa.sdk.source.source import PullResult
from sources.geo_entrez.config import GEOEntrezConfig

logger = logging.getLogger(__name__)

# Retry configuration
MAX_RETRIES = 5
BASE_DELAY = 1.0  # seconds
MAX_DELAY = 60.0  # seconds


class GEOEntrezSource:
    """Pulls GEO metadata from NCBI via E-utilities (Entrez).

    Supports both GSE (Series, ~230k records) and GDS (DataSets, ~5k curated).
    Best for incremental updates; for bulk initialization consider geo-ftp.
    """

    name = "geo-entrez"
    config_class = GEOEntrezConfig

    def __init__(self, config: GEOEntrezConfig) -> None:
        self._config = config
        self._client = httpx.AsyncClient(timeout=30.0)
        self._record_prefix = config.record_type.upper()  # "GSE" or "GDS"

    async def pull(
        self,
        since: datetime | None = None,
        limit: int | None = None,
        offset: int = 0,
        session: dict[str, Any] | None = None,
    ) -> PullResult:
        """Pull records from GEO with efficient chunked pagination.

        Uses NCBI's usehistory feature (WebEnv) to store search results server-side,
        enabling efficient O(1) access to any chunk without re-fetching all UIDs.

        Args:
            since: Only fetch records updated after this time.
            limit: Maximum number of records to fetch.
            offset: Skip first N records (for chunked processing).
            session: NCBI session state containing WebEnv, QueryKey, and total_count.

        Returns:
            Tuple of (records_iterator, session_for_next_chunk).
        """
        # Build search query
        query = f"{self._record_prefix}[ETYP]"
        if since:
            date_str = since.strftime("%Y/%m/%d")
            query += f" AND {date_str}:3000[PDAT]"

        # Initialize session with usehistory if not provided
        if session is None:
            web_env, query_key, total_count = await self._init_search_history(query)
            session = {
                "web_env": web_env,
                "query_key": query_key,
                "total_count": total_count,
            }
            logger.info(
                f"Initialized NCBI search session: {total_count} total records, "
                f"offset={offset}, limit={limit}"
            )
        else:
            total_count = session["total_count"]

        # Calculate how many UIDs to fetch for this chunk
        effective_limit = limit if limit is not None else total_count - offset
        logger.info(
            f"Fetching UIDs: offset={offset}, limit={limit}, effective_limit={effective_limit}, "
            f"total_count={total_count}"
        )

        if effective_limit <= 0:
            logger.warning(f"No UIDs to fetch (effective_limit={effective_limit})")

            async def empty_generator() -> AsyncIterator[UpstreamRecord]:
                return
                yield  # Make it a generator

            return empty_generator(), session

        uids = await self._fetch_uids_from_history(
            web_env=session["web_env"],
            query_key=session["query_key"],
            offset=offset,
            limit=effective_limit,
        )
        logger.info(f"Fetched {len(uids)} UIDs from NCBI history")

        async def generate() -> AsyncIterator[UpstreamRecord]:
            # Fetch metadata in batches
            for batch_start in range(0, len(uids), self._config.batch_size):
                batch_uids = uids[batch_start : batch_start + self._config.batch_size]
                records = await self._fetch_batch(batch_uids)
                for record in records:
                    yield record

        return generate(), session

    async def get_one(self, source_id: str) -> UpstreamRecord | None:
        """Fetch a single GSE by accession.

        Args:
            source_id: GSE accession (e.g., "GSE12345").

        Returns:
            RawRecord if found, None otherwise.
        """
        # Search for the specific accession
        query = f"{source_id}[ACCN]"
        uids = await self._search_uids(query, limit=1)

        if not uids:
            return None

        records = await self._fetch_batch(uids)
        return records[0] if records else None

    async def health(self) -> bool:
        """Check if GEO API is reachable."""
        try:
            params = self._base_params()
            params["db"] = "gds"
            resp = await self._client.get(
                f"{self._config.base_url}/einfo.fcgi",
                params=params,
            )
            return resp.status_code == 200
        except httpx.HTTPError:
            return False

    async def close(self) -> None:
        """Close the HTTP client."""
        await self._client.aclose()

    def _base_params(self) -> dict[str, str]:
        """Build base parameters required by NCBI."""
        params = {
            "tool": self._config.tool_name,
            "email": self._config.email,
        }
        if self._config.api_key:
            params["api_key"] = self._config.api_key
        return params

    async def _request_with_retry(self, url: str, params: dict[str, str]) -> httpx.Response:
        """Make HTTP request with retry on 429/5xx errors.

        Uses exponential backoff with jitter. Respects Retry-After header if present.
        """
        last_exception: Exception | None = None

        for attempt in range(MAX_RETRIES):
            try:
                resp = await self._client.get(url, params=params)

                # Success - return immediately
                if resp.status_code < 400:
                    return resp

                # Client error (except 429) - don't retry
                if 400 <= resp.status_code < 500 and resp.status_code != 429:
                    resp.raise_for_status()

                # Rate limited (429) or server error (5xx) - retry with backoff
                if resp.status_code == 429 or resp.status_code >= 500:
                    # Check for Retry-After header
                    retry_after = resp.headers.get("Retry-After")
                    if retry_after:
                        try:
                            delay = float(retry_after)
                        except ValueError:
                            delay = BASE_DELAY * (2**attempt)
                    else:
                        # Exponential backoff with jitter
                        delay = min(BASE_DELAY * (2**attempt) + random.uniform(0, 1), MAX_DELAY)

                    logger.warning(
                        f"NCBI API returned {resp.status_code}, "
                        f"retrying in {delay:.1f}s (attempt {attempt + 1}/{MAX_RETRIES})"
                    )
                    await asyncio.sleep(delay)
                    continue

            except httpx.TimeoutException as e:
                last_exception = e
                delay = min(BASE_DELAY * (2**attempt) + random.uniform(0, 1), MAX_DELAY)
                logger.warning(
                    f"NCBI API timeout, retrying in {delay:.1f}s "
                    f"(attempt {attempt + 1}/{MAX_RETRIES})"
                )
                await asyncio.sleep(delay)
                continue

            except httpx.HTTPError as e:
                # Network errors - retry with backoff
                last_exception = e
                delay = min(BASE_DELAY * (2**attempt) + random.uniform(0, 1), MAX_DELAY)
                logger.warning(
                    f"NCBI API error: {e}, retrying in {delay:.1f}s "
                    f"(attempt {attempt + 1}/{MAX_RETRIES})"
                )
                await asyncio.sleep(delay)
                continue

        # All retries exhausted - make final attempt and let it raise
        logger.error(f"NCBI API failed after {MAX_RETRIES} retries")
        if last_exception:
            raise last_exception
        resp = await self._client.get(url, params=params)
        resp.raise_for_status()
        return resp

    async def _search_uids(self, query: str, limit: int | None) -> list[str]:
        """Search GEO for UIDs matching the query.

        Uses pagination to fetch all results when limit is None,
        since NCBI caps retmax at 10,000 per request.
        """
        batch_size = 10000  # NCBI max per request

        # If limit is set and <= batch_size, single request is enough
        if limit is not None and limit <= batch_size:
            return await self._search_uids_single(query, limit)

        # Otherwise, paginate through all results
        all_uids: list[str] = []
        retstart = 0
        target = limit  # None means get everything

        while True:
            # Calculate how many to fetch this batch
            if target is not None:
                remaining = target - len(all_uids)
                fetch_count = min(batch_size, remaining)
            else:
                fetch_count = batch_size

            params = self._base_params()
            params.update(
                {
                    "db": "gds",
                    "term": query,
                    "retmax": str(fetch_count),
                    "retstart": str(retstart),
                    "usehistory": "n",
                }
            )

            resp = await self._request_with_retry(
                f"{self._config.base_url}/esearch.fcgi",
                params=params,
            )

            tree = ElementTree.fromstring(resp.text)
            uids = [id_elem.text for id_elem in tree.findall(".//Id") if id_elem.text]

            if not uids:
                break  # No more results

            all_uids.extend(uids)
            retstart += len(uids)

            target_str = str(target) if target else "all"
            logger.info(f"Fetched {len(all_uids)} UIDs so far (target: {target_str})")

            # Check if we've hit the limit or got all results
            if target is not None and len(all_uids) >= target:
                break
            if len(uids) < fetch_count:
                break  # Got fewer than requested, no more results

        return all_uids[:target] if target else all_uids

    async def _init_search_history(self, query: str) -> tuple[str, str, int]:
        """Initialize search with usehistory=y, storing results on NCBI server.

        Returns:
            Tuple of (WebEnv, QueryKey, total_count).
        """
        params = self._base_params()
        params.update(
            {
                "db": "gds",
                "term": query,
                "usehistory": "y",  # Store results on NCBI server
                "retmax": "0",  # Don't fetch UIDs yet, just get count
            }
        )

        resp = await self._request_with_retry(
            f"{self._config.base_url}/esearch.fcgi",
            params=params,
        )

        tree = ElementTree.fromstring(resp.text)
        web_env = tree.findtext("WebEnv") or ""
        query_key = tree.findtext("QueryKey") or ""
        total_count = int(tree.findtext("Count") or "0")

        if not web_env or not query_key:
            raise ValueError("NCBI did not return WebEnv/QueryKey for usehistory query")

        return web_env, query_key, total_count

    async def _fetch_uids_from_history(
        self, web_env: str, query_key: str, offset: int, limit: int
    ) -> list[str]:
        """Fetch UIDs for a specific range using stored search results (WebEnv).

        This avoids re-running the search for each chunk, making pagination O(1).
        """
        params = self._base_params()
        params.update(
            {
                "db": "gds",
                "WebEnv": web_env,
                "query_key": query_key,
                "retstart": str(offset),
                "retmax": str(limit),
            }
        )

        resp = await self._request_with_retry(
            f"{self._config.base_url}/esearch.fcgi",
            params=params,
        )

        tree = ElementTree.fromstring(resp.text)
        return [id_elem.text for id_elem in tree.findall(".//Id") if id_elem.text]

    async def _search_uids_single(self, query: str, limit: int) -> list[str]:
        """Single request for small limits."""
        params = self._base_params()
        params.update(
            {
                "db": "gds",
                "term": query,
                "retmax": str(limit),
                "usehistory": "n",
            }
        )

        resp = await self._request_with_retry(
            f"{self._config.base_url}/esearch.fcgi",
            params=params,
        )

        tree = ElementTree.fromstring(resp.text)
        return [id_elem.text for id_elem in tree.findall(".//Id") if id_elem.text]

    async def _fetch_batch(self, uids: list[str]) -> list[UpstreamRecord]:
        """Fetch metadata for a batch of UIDs via ESummary."""
        if not uids:
            return []

        params = self._base_params()
        params.update(
            {
                "db": "gds",
                "id": ",".join(uids),
                "version": "2.0",
            }
        )

        resp = await self._request_with_retry(
            f"{self._config.base_url}/esummary.fcgi",
            params=params,
        )

        return self._parse_esummary(resp.text)

    def _parse_esummary(self, xml_text: str) -> list[UpstreamRecord]:
        """Parse ESummary XML response into UpstreamRecords."""
        tree = ElementTree.fromstring(xml_text)
        records = []
        now = datetime.now(UTC)

        for doc in tree.findall(".//DocumentSummary"):
            accession = self._get_item(doc, "Accession")
            if not accession or not accession.startswith(self._record_prefix):
                continue

            metadata = {
                "title": self._get_item(doc, "title"),
                "summary": self._get_item(doc, "summary"),
                "organism": self._get_item(doc, "taxon"),
                "platform": self._get_item(doc, "GPL"),
                "sample_count": self._get_item(doc, "n_samples"),
                "pub_date": self._get_item(doc, "PDAT"),
                "entry_type": self._get_item(doc, "entryType"),
                "gds_type": self._get_item(doc, "gdsType"),
            }

            # Filter out None values
            metadata = {k: v for k, v in metadata.items() if v is not None}

            records.append(
                UpstreamRecord(
                    source_id=accession,
                    source_type=self.name,
                    metadata=metadata,
                    fetched_at=now,
                    source_url=f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={accession}",
                )
            )

        return records

    def _get_item(self, doc: ElementTree.Element, name: str) -> str | None:
        """Extract an item value from a DocumentSummary element."""
        # Try as attribute first
        elem = doc.find(f"Item[@Name='{name}']")
        if elem is not None and elem.text:
            return elem.text.strip()

        # Try as direct child element
        elem = doc.find(name)
        if elem is not None and elem.text:
            return elem.text.strip()

        return None
