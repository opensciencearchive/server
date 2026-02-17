"""DI provider for HTTP infrastructure."""

from typing import NewType

import httpx
from dishka import provide

from osa.domain.semantics.port.ontology_fetcher import OntologyFetcher
from osa.infrastructure.http.ontology_fetcher import HttpOntologyFetcher
from osa.util.di.base import Provider
from osa.util.di.scope import Scope

# Disambiguate from the auth httpx.AsyncClient
OntologyHttpClient = NewType("OntologyHttpClient", httpx.AsyncClient)

# Longer read timeout for large ontology files (e.g. Gene Ontology)
_ONTOLOGY_TIMEOUT = httpx.Timeout(
    connect=5.0,
    read=30.0,
    write=5.0,
    pool=5.0,
)


class HttpProvider(Provider):
    """DI provider for HTTP fetcher adapters."""

    @provide(scope=Scope.APP)
    def get_ontology_http_client(self) -> OntologyHttpClient:
        """Dedicated HTTP client for fetching ontology files."""
        return OntologyHttpClient(httpx.AsyncClient(timeout=_ONTOLOGY_TIMEOUT))

    @provide(scope=Scope.APP, provides=OntologyFetcher)
    def get_ontology_fetcher(self, client: OntologyHttpClient) -> HttpOntologyFetcher:
        return HttpOntologyFetcher(client=client)
