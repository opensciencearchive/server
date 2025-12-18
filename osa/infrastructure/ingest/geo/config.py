"""Configuration for GEO E-utilities ingestor."""

from pydantic import EmailStr

from osa.sdk.ingest.config import IngestorConfig


class GEOIngestorConfig(IngestorConfig):
    """Configuration for GEO E-utilities ingestor."""

    api_key: str | None = None  # NCBI API key (optional, increases rate limit)
    email: EmailStr  # Required by NCBI
    tool_name: str = "osa"  # Application identifier
    batch_size: int = 100  # Records per ESummary request
    base_url: str = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
