"""Unit tests for ImportOntology command handler."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.auth.model.value import ProviderIdentity, UserId
from osa.domain.semantics.command.import_ontology import (
    ImportOntology,
    ImportOntologyHandler,
    ImportOntologyResult,
)
from osa.domain.semantics.model.ontology import Ontology, Term
from osa.domain.shared.error import AuthorizationError
from osa.domain.shared.model.srn import Domain, LocalId, OntologySRN, Semver


def _make_principal(roles: frozenset[Role]) -> Principal:
    return Principal(
        user_id=UserId.generate(),
        provider_identity=ProviderIdentity(provider="test", external_id="ext"),
        roles=roles,
    )


def _make_ontology() -> Ontology:
    return Ontology(
        srn=OntologySRN(
            domain=Domain("localhost"),
            id=LocalId("test-id"),
            version=Semver.from_string("1.0.0"),
        ),
        title="Test Ontology",
        description="A test.",
        terms=[Term(term_id="T:001", label="Root")],
        created_at=datetime.now(UTC),
    )


def _obographs_data() -> dict:
    return {
        "graphs": [
            {
                "id": "http://example.org/test.owl",
                "lbl": "Test Ontology",
                "nodes": [{"id": "T:001", "lbl": "Root", "type": "CLASS"}],
            }
        ]
    }


class TestImportOntologyHandler:
    @pytest.mark.asyncio
    async def test_fetches_and_imports(self):
        admin = _make_principal(frozenset({Role.ADMIN}))
        ontology = _make_ontology()

        fetcher = AsyncMock()
        fetcher.fetch_json.return_value = _obographs_data()

        service = AsyncMock()
        service.import_from_obographs.return_value = ontology

        handler = ImportOntologyHandler(
            principal=admin,
            ontology_service=service,
            fetcher=fetcher,
        )

        result = await handler.run(ImportOntology(url="https://example.com/onto.json"))

        assert isinstance(result, ImportOntologyResult)
        assert result.srn == ontology.srn
        assert result.title == "Test Ontology"
        fetcher.fetch_json.assert_called_once_with("https://example.com/onto.json")
        service.import_from_obographs.assert_called_once()

    @pytest.mark.asyncio
    async def test_passes_version_override(self):
        admin = _make_principal(frozenset({Role.ADMIN}))
        ontology = _make_ontology()

        fetcher = AsyncMock()
        fetcher.fetch_json.return_value = _obographs_data()

        service = AsyncMock()
        service.import_from_obographs.return_value = ontology

        handler = ImportOntologyHandler(
            principal=admin,
            ontology_service=service,
            fetcher=fetcher,
        )

        await handler.run(ImportOntology(url="https://example.com/onto.json", version="2.0.0"))

        call_kwargs = service.import_from_obographs.call_args
        assert call_kwargs[1]["version_override"] == "2.0.0"

    @pytest.mark.asyncio
    async def test_requires_admin_role(self):
        depositor = _make_principal(frozenset({Role.DEPOSITOR}))
        handler = ImportOntologyHandler(
            principal=depositor,
            ontology_service=AsyncMock(),
            fetcher=AsyncMock(),
        )

        with pytest.raises(AuthorizationError) as exc_info:
            await handler.run(ImportOntology(url="https://example.com/onto.json"))
        assert exc_info.value.code == "access_denied"

    @pytest.mark.asyncio
    async def test_rejects_unauthenticated(self):
        handler = ImportOntologyHandler.__new__(ImportOntologyHandler)

        with pytest.raises(AuthorizationError) as exc_info:
            await handler.run(ImportOntology(url="https://example.com/onto.json"))
        assert exc_info.value.code == "missing_token"

    @pytest.mark.asyncio
    async def test_propagates_fetch_errors(self):
        admin = _make_principal(frozenset({Role.ADMIN}))
        fetcher = AsyncMock()
        fetcher.fetch_json.side_effect = RuntimeError("Connection failed")

        handler = ImportOntologyHandler(
            principal=admin,
            ontology_service=AsyncMock(),
            fetcher=fetcher,
        )

        with pytest.raises(RuntimeError, match="Connection failed"):
            await handler.run(ImportOntology(url="https://example.com/onto.json"))
