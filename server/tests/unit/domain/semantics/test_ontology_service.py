"""Unit tests for OntologyService."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from osa.domain.semantics.model.ontology import Ontology, Term
from osa.domain.semantics.service.ontology import OntologyService
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.model.srn import Domain, OntologySRN


def _make_srn(id: str = "test-onto", version: str = "1.0.0") -> OntologySRN:
    return OntologySRN.parse(f"urn:osa:localhost:onto:{id}@{version}")


def _make_ontology(srn: OntologySRN | None = None) -> Ontology:
    return Ontology(
        srn=srn or _make_srn(),
        title="Sex",
        terms=[Term(term_id="male", label="Male"), Term(term_id="female", label="Female")],
        created_at=datetime.now(UTC),
    )


class TestOntologyServiceCreate:
    @pytest.mark.asyncio
    async def test_create_ontology(self):
        repo = AsyncMock()
        service = OntologyService(
            ontology_repo=repo,
            node_domain=Domain("localhost"),
        )
        result = await service.create_ontology(
            title="Sex",
            version="1.0.0",
            terms=[Term(term_id="male", label="Male")],
            description="Biological sex",
        )
        assert result.title == "Sex"
        repo.save.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_ontology_generates_srn(self):
        repo = AsyncMock()
        service = OntologyService(
            ontology_repo=repo,
            node_domain=Domain("localhost"),
        )
        result = await service.create_ontology(
            title="Test",
            version="1.0.0",
            terms=[Term(term_id="a", label="A")],
        )
        assert str(result.srn).startswith("urn:osa:localhost:onto:")
        assert str(result.srn).endswith("@1.0.0")


class TestOntologyServiceGet:
    @pytest.mark.asyncio
    async def test_get_existing(self):
        onto = _make_ontology()
        repo = AsyncMock()
        repo.get.return_value = onto
        service = OntologyService(ontology_repo=repo, node_domain=Domain("localhost"))

        result = await service.get_ontology(onto.srn)
        assert result == onto

    @pytest.mark.asyncio
    async def test_get_nonexistent_raises(self):
        repo = AsyncMock()
        repo.get.return_value = None
        service = OntologyService(ontology_repo=repo, node_domain=Domain("localhost"))

        with pytest.raises(NotFoundError):
            await service.get_ontology(_make_srn())


class TestOntologyServiceList:
    @pytest.mark.asyncio
    async def test_list_ontologies(self):
        onto = _make_ontology()
        repo = AsyncMock()
        repo.list.return_value = [onto]
        service = OntologyService(ontology_repo=repo, node_domain=Domain("localhost"))

        result = await service.list_ontologies()
        assert len(result) == 1
        assert result[0] == onto


def _obographs_data(
    *,
    lbl: str = "Test Ontology",
    version: str | None = "2.1.0",
    description: str | None = None,
) -> dict:
    """Build minimal OBO Graphs data for service tests."""
    graph: dict = {
        "id": "http://example.org/test.owl",
        "lbl": lbl,
        "nodes": [
            {"id": "T:001", "lbl": "Root", "type": "CLASS"},
            {"id": "T:002", "lbl": "Child", "type": "CLASS"},
        ],
        "edges": [{"sub": "T:002", "pred": "is_a", "obj": "T:001"}],
    }
    meta: dict = {}
    if version is not None:
        meta["version"] = version
    if description is not None:
        meta["definition"] = {"val": description}
    if meta:
        graph["meta"] = meta
    return {"graphs": [graph]}


class TestOntologyServiceImport:
    @pytest.mark.asyncio
    async def test_import_from_obographs_creates_ontology(self):
        repo = AsyncMock()
        service = OntologyService(ontology_repo=repo, node_domain=Domain("localhost"))

        result = await service.import_from_obographs(_obographs_data())

        assert result.title == "Test Ontology"
        assert len(result.terms) == 2
        repo.save.assert_called_once()

    @pytest.mark.asyncio
    async def test_import_uses_version_from_obo_data(self):
        repo = AsyncMock()
        service = OntologyService(ontology_repo=repo, node_domain=Domain("localhost"))

        result = await service.import_from_obographs(_obographs_data(version="2.1.0"))

        assert str(result.srn).endswith("@2.1.0")

    @pytest.mark.asyncio
    async def test_import_uses_version_override(self):
        repo = AsyncMock()
        service = OntologyService(ontology_repo=repo, node_domain=Domain("localhost"))

        result = await service.import_from_obographs(
            _obographs_data(version="2024-01-15"),
            version_override="3.0.0",
        )

        assert str(result.srn).endswith("@3.0.0")

    @pytest.mark.asyncio
    async def test_import_falls_back_to_1_0_0(self):
        repo = AsyncMock()
        service = OntologyService(ontology_repo=repo, node_domain=Domain("localhost"))

        result = await service.import_from_obographs(_obographs_data(version=None))

        assert str(result.srn).endswith("@1.0.0")

    @pytest.mark.asyncio
    async def test_import_passes_description(self):
        repo = AsyncMock()
        service = OntologyService(ontology_repo=repo, node_domain=Domain("localhost"))

        result = await service.import_from_obographs(
            _obographs_data(description="A test ontology.")
        )

        assert result.description == "A test ontology."
