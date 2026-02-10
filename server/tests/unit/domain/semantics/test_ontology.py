"""Unit tests for Ontology aggregate."""

from datetime import UTC, datetime

import pytest

from osa.domain.semantics.model.ontology import Ontology, Term
from osa.domain.shared.error import ValidationError
from osa.domain.shared.model.srn import OntologySRN


def _make_srn(id: str = "test-onto", version: str = "1.0.0") -> OntologySRN:
    return OntologySRN.parse(f"urn:osa:localhost:onto:{id}@{version}")


def _make_term(term_id: str = "male", label: str = "Male") -> Term:
    return Term(term_id=term_id, label=label)


class TestOntologyCreation:
    def test_create_with_single_term(self):
        onto = Ontology(
            srn=_make_srn(),
            title="Sex",
            terms=[_make_term()],
            created_at=datetime.now(UTC),
        )
        assert onto.title == "Sex"
        assert len(onto.terms) == 1

    def test_create_with_multiple_terms(self):
        onto = Ontology(
            srn=_make_srn(),
            title="Sex",
            terms=[
                _make_term("male", "Male"),
                _make_term("female", "Female"),
                _make_term("mixed", "Mixed"),
            ],
            created_at=datetime.now(UTC),
        )
        assert len(onto.terms) == 3

    def test_create_with_description(self):
        onto = Ontology(
            srn=_make_srn(),
            title="Sex",
            description="Biological sex categories",
            terms=[_make_term()],
            created_at=datetime.now(UTC),
        )
        assert onto.description == "Biological sex categories"

    def test_create_with_optional_term_fields(self):
        term = Term(
            term_id="neocortex",
            label="Neocortex",
            synonyms=["isocortex", "neopallium"],
            parent_ids=["UBERON:0000955"],
            definition="Part of cerebral cortex",
            deprecated=False,
        )
        onto = Ontology(
            srn=_make_srn(),
            title="Brain Regions",
            terms=[term],
            created_at=datetime.now(UTC),
        )
        assert onto.terms[0].synonyms == ["isocortex", "neopallium"]
        assert onto.terms[0].parent_ids == ["UBERON:0000955"]


class TestOntologyInvariants:
    def test_rejects_empty_terms(self):
        with pytest.raises(ValidationError, match="at least one term"):
            Ontology(
                srn=_make_srn(),
                title="Empty",
                terms=[],
                created_at=datetime.now(UTC),
            )

    def test_rejects_duplicate_term_ids(self):
        with pytest.raises(ValidationError, match="Duplicate term IDs"):
            Ontology(
                srn=_make_srn(),
                title="Bad",
                terms=[_make_term("male", "Male"), _make_term("male", "Male Duplicate")],
                created_at=datetime.now(UTC),
            )


class TestOntologyImmutability:
    def test_srn_is_set(self):
        onto = Ontology(
            srn=_make_srn("sex-onto", "1.0.0"),
            title="Sex",
            terms=[_make_term()],
            created_at=datetime.now(UTC),
        )
        assert str(onto.srn) == "urn:osa:localhost:onto:sex-onto@1.0.0"
