"""Unit tests for OBO Graphs JSON parser."""

import json
from pathlib import Path

import pytest

from osa.domain.semantics.util.obographs import ParsedOntology, parse_obographs


def _find_server_root() -> Path:
    path = Path(__file__).resolve()
    while path != path.parent:
        if (path / "pyproject.toml").exists():
            return path
        path = path.parent
    raise RuntimeError("Could not find pyproject.toml above test file")


ONTOLOGIES_DIR = _find_server_root() / "ontologies"


def _minimal_graph(
    *,
    graph_id: str = "http://purl.obolibrary.org/obo/test.owl",
    lbl: str | None = "Test Ontology",
    nodes: list[dict] | None = None,
    edges: list[dict] | None = None,
    meta: dict | None = None,
) -> dict:
    """Build a minimal OBO Graphs JSON structure."""
    graph: dict = {"id": graph_id}
    if lbl is not None:
        graph["lbl"] = lbl
    if nodes is not None:
        graph["nodes"] = nodes
    if edges is not None:
        graph["edges"] = edges
    if meta is not None:
        graph["meta"] = meta
    return {"graphs": [graph]}


def _class_node(
    node_id: str,
    label: str,
    *,
    definition: str | None = None,
    synonyms: list[str] | None = None,
    deprecated: bool = False,
) -> dict:
    """Build a CLASS node for OBO Graphs."""
    node: dict = {"id": node_id, "lbl": label, "type": "CLASS"}
    meta: dict = {}
    if definition is not None:
        meta["definition"] = {"val": definition}
    if synonyms is not None:
        meta["synonyms"] = [{"val": s} for s in synonyms]
    if deprecated:
        meta["deprecated"] = True
    if meta:
        node["meta"] = meta
    return node


class TestParseMinimalGraph:
    def test_parses_single_node(self):
        data = _minimal_graph(nodes=[_class_node("T:001", "Root")])
        result = parse_obographs(data)

        assert isinstance(result, ParsedOntology)
        assert len(result.terms) == 1
        assert result.terms[0].term_id == "T:001"
        assert result.terms[0].label == "Root"

    def test_extracts_title_from_graph_lbl(self):
        data = _minimal_graph(lbl="Biological Sex", nodes=[_class_node("T:001", "Root")])
        result = parse_obographs(data)
        assert result.title == "Biological Sex"

    def test_falls_back_to_graph_id_when_no_lbl(self):
        data = _minimal_graph(
            graph_id="http://purl.obolibrary.org/obo/pato.owl",
            lbl=None,
            nodes=[_class_node("T:001", "Root")],
        )
        result = parse_obographs(data)
        assert result.title == "http://purl.obolibrary.org/obo/pato.owl"


class TestVersionExtraction:
    def test_extracts_version_from_graph_meta(self):
        data = _minimal_graph(
            nodes=[_class_node("T:001", "Root")],
            meta={"version": "2024-01-15"},
        )
        result = parse_obographs(data)
        assert result.version == "2024-01-15"

    def test_version_is_none_when_no_meta(self):
        data = _minimal_graph(nodes=[_class_node("T:001", "Root")])
        result = parse_obographs(data)
        assert result.version is None


class TestDescriptionExtraction:
    def test_extracts_description_from_meta_definition(self):
        data = _minimal_graph(
            nodes=[_class_node("T:001", "Root")],
            meta={"definition": {"val": "An ontology for testing."}},
        )
        result = parse_obographs(data)
        assert result.description == "An ontology for testing."

    def test_description_is_none_when_no_definition(self):
        data = _minimal_graph(nodes=[_class_node("T:001", "Root")])
        result = parse_obographs(data)
        assert result.description is None


class TestTermMapping:
    def test_maps_definition(self):
        data = _minimal_graph(nodes=[_class_node("T:001", "Root", definition="The root term.")])
        result = parse_obographs(data)
        assert result.terms[0].definition == "The root term."

    def test_maps_synonyms(self):
        data = _minimal_graph(nodes=[_class_node("T:001", "Root", synonyms=["Base", "Top"])])
        result = parse_obographs(data)
        assert result.terms[0].synonyms == ["Base", "Top"]

    def test_maps_deprecated_flag(self):
        data = _minimal_graph(nodes=[_class_node("T:001", "Obsolete", deprecated=True)])
        result = parse_obographs(data)
        assert result.terms[0].deprecated is True

    def test_defaults_to_not_deprecated(self):
        data = _minimal_graph(nodes=[_class_node("T:001", "Root")])
        result = parse_obographs(data)
        assert result.terms[0].deprecated is False

    def test_empty_synonyms_when_none(self):
        data = _minimal_graph(nodes=[_class_node("T:001", "Root")])
        result = parse_obographs(data)
        assert result.terms[0].synonyms == []

    def test_empty_parent_ids_when_no_edges(self):
        data = _minimal_graph(nodes=[_class_node("T:001", "Root")])
        result = parse_obographs(data)
        assert result.terms[0].parent_ids == []


class TestEdgeMapping:
    def test_builds_parent_ids_from_is_a_edges(self):
        data = _minimal_graph(
            nodes=[
                _class_node("T:001", "Root"),
                _class_node("T:002", "Child"),
            ],
            edges=[{"sub": "T:002", "pred": "is_a", "obj": "T:001"}],
        )
        result = parse_obographs(data)
        child = next(t for t in result.terms if t.term_id == "T:002")
        assert child.parent_ids == ["T:001"]

    def test_ignores_non_is_a_edges(self):
        data = _minimal_graph(
            nodes=[
                _class_node("T:001", "Root"),
                _class_node("T:002", "Child"),
            ],
            edges=[
                {"sub": "T:002", "pred": "is_a", "obj": "T:001"},
                {"sub": "T:002", "pred": "part_of", "obj": "T:001"},
            ],
        )
        result = parse_obographs(data)
        child = next(t for t in result.terms if t.term_id == "T:002")
        assert child.parent_ids == ["T:001"]

    def test_multiple_parents(self):
        data = _minimal_graph(
            nodes=[
                _class_node("T:001", "Root A"),
                _class_node("T:002", "Root B"),
                _class_node("T:003", "Child"),
            ],
            edges=[
                {"sub": "T:003", "pred": "is_a", "obj": "T:001"},
                {"sub": "T:003", "pred": "is_a", "obj": "T:002"},
            ],
        )
        result = parse_obographs(data)
        child = next(t for t in result.terms if t.term_id == "T:003")
        assert sorted(child.parent_ids) == ["T:001", "T:002"]


class TestFiltering:
    def test_skips_property_nodes(self):
        data = _minimal_graph(
            nodes=[
                _class_node("T:001", "Root"),
                {"id": "P:001", "lbl": "has_part", "type": "PROPERTY"},
            ]
        )
        result = parse_obographs(data)
        assert len(result.terms) == 1
        assert result.terms[0].term_id == "T:001"

    def test_skips_individual_nodes(self):
        data = _minimal_graph(
            nodes=[
                _class_node("T:001", "Root"),
                {"id": "I:001", "lbl": "Instance1", "type": "INDIVIDUAL"},
            ]
        )
        result = parse_obographs(data)
        assert len(result.terms) == 1

    def test_skips_nodes_without_label(self):
        data = _minimal_graph(
            nodes=[
                _class_node("T:001", "Root"),
                {"id": "T:002", "type": "CLASS"},  # no lbl
            ]
        )
        result = parse_obographs(data)
        assert len(result.terms) == 1
        assert result.terms[0].term_id == "T:001"

    def test_skips_nodes_without_type(self):
        """Nodes without explicit type should be skipped."""
        data = _minimal_graph(
            nodes=[
                _class_node("T:001", "Root"),
                {"id": "T:002", "lbl": "Mystery"},  # no type
            ]
        )
        result = parse_obographs(data)
        assert len(result.terms) == 1


class TestValidation:
    def test_rejects_data_with_no_graphs_key(self):
        with pytest.raises(ValueError, match="graphs"):
            parse_obographs({"nodes": []})

    def test_rejects_empty_graphs_list(self):
        with pytest.raises(ValueError, match="graphs"):
            parse_obographs({"graphs": []})

    def test_rejects_graph_with_no_class_nodes(self):
        data = _minimal_graph(nodes=[{"id": "P:001", "lbl": "has_part", "type": "PROPERTY"}])
        with pytest.raises(ValueError, match="no CLASS"):
            parse_obographs(data)

    def test_rejects_graph_with_no_nodes(self):
        data = _minimal_graph(nodes=[])
        with pytest.raises(ValueError, match="no CLASS"):
            parse_obographs(data)


class TestSampleOntologies:
    """Verify bundled sample ontology files parse correctly."""

    def test_biological_sex_ontology(self):
        data = json.loads((ONTOLOGIES_DIR / "biological-sex.obographs.json").read_text())
        result = parse_obographs(data)

        assert result.title == "Biological Sex"
        assert result.version == "1.0.0"
        assert result.description is not None
        assert len(result.terms) == 5

        root = next(t for t in result.terms if t.term_id == "OSAO:0000001")
        assert root.label == "biological sex"
        assert root.parent_ids == []

        female = next(t for t in result.terms if t.term_id == "OSAO:0000002")
        assert female.label == "female"
        assert female.parent_ids == ["OSAO:0000001"]
        assert "F" in female.synonyms

    def test_license_ontology(self):
        data = json.loads((ONTOLOGIES_DIR / "license.obographs.json").read_text())
        result = parse_obographs(data)

        assert result.title == "License"
        assert result.version == "1.0.0"
        assert len(result.terms) == 7

        cc0 = next(t for t in result.terms if t.term_id == "OSAO:1000002")
        assert cc0.label == "CC0 1.0"
        assert cc0.parent_ids == ["OSAO:1000001"]
        assert "CC0" in cc0.synonyms
