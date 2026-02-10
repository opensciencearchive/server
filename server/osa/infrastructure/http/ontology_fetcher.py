"""HTTP adapter for OntologyFetcher port."""

import httpx

from osa.domain.semantics.port.ontology_fetcher import OntologyFetcher


class HttpOntologyFetcher(OntologyFetcher):
    """Fetches ontology JSON from a URL using httpx."""

    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def fetch_json(self, url: str) -> dict:
        response = await self._client.get(url)
        response.raise_for_status()
        return response.json()
