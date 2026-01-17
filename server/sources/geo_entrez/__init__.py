"""GEO Entrez source - uses NCBI E-utilities API."""

from sources.geo_entrez.config import GEOEntrezConfig
from sources.geo_entrez.source import GEOEntrezSource

__all__ = ["GEOEntrezSource", "GEOEntrezConfig"]
