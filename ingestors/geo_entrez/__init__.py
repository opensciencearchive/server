"""GEO Entrez ingestor - uses NCBI E-utilities API."""

from ingestors.geo_entrez.config import GEOEntrezConfig
from ingestors.geo_entrez.ingestor import GEOEntrezIngestor

__all__ = ["GEOEntrezIngestor", "GEOEntrezConfig"]
