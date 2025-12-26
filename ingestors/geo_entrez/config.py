"""Configuration for GEO Entrez ingestor."""

from typing import Literal

from pydantic import EmailStr

from osa.sdk.ingest.config import IngestorConfig

GEORecordType = Literal["gse", "gds"]


class GEOEntrezConfig(IngestorConfig):
    """Configuration for GEO Entrez (E-utilities) ingestor.

    Uses NCBI E-utilities API for incremental updates.
    """

    record_type: GEORecordType = "gse"  # GSE (~230k) or GDS (~5k curated)
    api_key: str | None = None  # NCBI API key (optional, increases rate limit)
    email: EmailStr  # Required by NCBI
    tool_name: str = "osa"  # Application identifier
    batch_size: int = 100  # Records per ESummary request
    base_url: str = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
