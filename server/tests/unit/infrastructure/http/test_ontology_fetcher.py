"""Unit tests for HttpOntologyFetcher adapter."""

from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from osa.infrastructure.http.ontology_fetcher import HttpOntologyFetcher


class TestHttpOntologyFetcher:
    @pytest.mark.asyncio
    async def test_fetches_and_parses_json(self):
        expected = {"graphs": [{"id": "test", "nodes": []}]}
        response = MagicMock(spec=httpx.Response)
        response.json.return_value = expected
        response.raise_for_status = MagicMock()

        client = AsyncMock(spec=httpx.AsyncClient)
        client.get.return_value = response

        fetcher = HttpOntologyFetcher(client=client)
        result = await fetcher.fetch_json("https://example.com/ontology.json")

        assert result == expected
        client.get.assert_called_once_with("https://example.com/ontology.json")
        response.raise_for_status.assert_called_once()

    @pytest.mark.asyncio
    async def test_raises_on_http_error(self):
        client = AsyncMock(spec=httpx.AsyncClient)
        response = MagicMock(spec=httpx.Response)
        response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Not Found",
            request=MagicMock(),
            response=MagicMock(status_code=404),
        )
        client.get.return_value = response

        fetcher = HttpOntologyFetcher(client=client)
        with pytest.raises(httpx.HTTPStatusError):
            await fetcher.fetch_json("https://example.com/missing.json")

    @pytest.mark.asyncio
    async def test_raises_on_invalid_json(self):
        client = AsyncMock(spec=httpx.AsyncClient)
        response = MagicMock(spec=httpx.Response)
        response.raise_for_status = MagicMock()
        response.json.side_effect = ValueError("Invalid JSON")
        client.get.return_value = response

        fetcher = HttpOntologyFetcher(client=client)
        with pytest.raises(ValueError, match="Invalid JSON"):
            await fetcher.fetch_json("https://example.com/bad.json")
