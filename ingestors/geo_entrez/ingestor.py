"""GEO Entrez ingestor - uses NCBI E-utilities API."""

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from xml.etree import ElementTree

import httpx

from ingestors.geo_entrez.config import GEOEntrezConfig
from osa.sdk.ingest.record import UpstreamRecord


class GEOEntrezIngestor:
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
    ) -> AsyncIterator[UpstreamRecord]:
        """Pull records from GEO.

        Args:
            since: Only fetch records updated after this time.
            limit: Maximum number of records to fetch.

        Yields:
            UpstreamRecord for each GEO record.
        """
        # Build search query
        query = f"{self._record_prefix}[ETYP]"
        if since:
            date_str = since.strftime("%Y/%m/%d")
            query += f" AND {date_str}:3000[PDAT]"

        # Get UIDs via esearch
        uids = await self._search_uids(query, limit)

        # Fetch metadata in batches
        for batch_start in range(0, len(uids), self._config.batch_size):
            batch_uids = uids[batch_start : batch_start + self._config.batch_size]
            records = await self._fetch_batch(batch_uids)
            for record in records:
                yield record

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

    async def _search_uids(self, query: str, limit: int | None) -> list[str]:
        """Search GEO for UIDs matching the query."""
        params = self._base_params()
        params.update(
            {
                "db": "gds",
                "term": query,
                "retmax": str(limit or 10000),
                "usehistory": "n",
            }
        )

        resp = await self._client.get(
            f"{self._config.base_url}/esearch.fcgi",
            params=params,
        )
        resp.raise_for_status()

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

        resp = await self._client.get(
            f"{self._config.base_url}/esummary.fcgi",
            params=params,
        )
        resp.raise_for_status()

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
