"""Port for fetching ontology data from external URLs."""

from abc import abstractmethod
from typing import Protocol

from osa.domain.shared.port import Port


class OntologyFetcher(Port, Protocol):
    """Fetches ontology JSON data from a URL."""

    @abstractmethod
    async def fetch_json(self, url: str) -> dict: ...
