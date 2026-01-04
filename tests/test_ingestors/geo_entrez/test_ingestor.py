"""Integration tests for GEO Entrez ingestor against live NCBI E-utilities API."""

from collections.abc import AsyncGenerator
from datetime import datetime

import pytest

from ingestors.geo_entrez import GEOEntrezConfig, GEOEntrezIngestor
from osa.sdk.ingest.record import UpstreamRecord


@pytest.fixture
def geo_config() -> GEOEntrezConfig:
    return GEOEntrezConfig(
        email="test@example.com",
        tool_name="osa-integration-test",
    )


@pytest.fixture
async def geo_ingestor(
    geo_config: GEOEntrezConfig,
) -> AsyncGenerator[GEOEntrezIngestor, None]:
    ingestor = GEOEntrezIngestor(geo_config)
    yield ingestor
    await ingestor.close()


class TestGEOEntrezIngestorIntegration:
    """Integration tests that hit the live GEO API."""

    async def test_health_returns_true(self, geo_ingestor: GEOEntrezIngestor) -> None:
        """Health check should return True when GEO API is reachable."""
        result = await geo_ingestor.health()
        assert result is True

    async def test_get_one_returns_upstream_record(self, geo_ingestor: GEOEntrezIngestor) -> None:
        """Fetching a known GSE should return a valid UpstreamRecord."""
        # GSE1 is one of the earliest GEO series, stable for testing
        record = await geo_ingestor.get_one("GSE1")

        assert record is not None
        assert isinstance(record, UpstreamRecord)
        assert record.source_id == "GSE1"
        assert record.source_type == "geo-entrez"
        assert isinstance(record.metadata, dict)
        assert isinstance(record.fetched_at, datetime)
        assert record.source_url is not None
        assert "GSE1" in record.source_url

        # Verify expected metadata fields are present
        assert "title" in record.metadata
        assert record.metadata["title"] is not None

    async def test_get_one_nonexistent_returns_none(self, geo_ingestor: GEOEntrezIngestor) -> None:
        """Fetching a nonexistent GSE should return None."""
        record = await geo_ingestor.get_one("GSE999999999999")
        assert record is None

    async def test_pull_yields_upstream_records(self, geo_ingestor: GEOEntrezIngestor) -> None:
        """Pulling records should yield valid UpstreamRecords."""
        records: list[UpstreamRecord] = []

        async for record in geo_ingestor.pull(limit=3):
            records.append(record)

        assert len(records) >= 1

        for record in records:
            assert isinstance(record, UpstreamRecord)
            assert record.source_id.startswith("GSE")
            assert record.source_type == "geo-entrez"
            assert isinstance(record.metadata, dict)
            assert isinstance(record.fetched_at, datetime)
