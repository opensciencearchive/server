"""Pure parser for OBO Graphs JSON format.

Converts OBO Graphs JSON (used by OBO Foundry ontologies like GO, PATO, etc.)
into OSA domain Term objects.

Spec: https://github.com/geneontology/obographs
"""

from collections import defaultdict
from dataclasses import dataclass, field

from osa.domain.semantics.model.ontology import Term


@dataclass
class ParsedOntology:
    """Result of parsing an OBO Graphs JSON document."""

    title: str
    description: str | None = None
    version: str | None = None
    terms: list[Term] = field(default_factory=list)


def parse_obographs(data: dict) -> ParsedOntology:
    """Parse an OBO Graphs JSON dict into a ParsedOntology.

    Args:
        data: Parsed JSON dict following the OBO Graphs JSON spec.

    Returns:
        ParsedOntology with title, description, version, and Term objects.

    Raises:
        ValueError: If the data is missing required keys or contains no CLASS nodes.
    """
    graphs = data.get("graphs")
    if not graphs:
        raise ValueError("OBO Graphs JSON must contain a non-empty 'graphs' key")

    graph = graphs[0]

    # Extract metadata
    title = graph.get("lbl") or graph.get("id", "Unknown")
    graph_meta = graph.get("meta", {})
    version = graph_meta.get("version")
    description_def = graph_meta.get("definition")
    description = description_def.get("val") if isinstance(description_def, dict) else None

    # Build parent_ids index from is_a edges
    parent_index: dict[str, list[str]] = defaultdict(list)
    for edge in graph.get("edges", []):
        if edge.get("pred") == "is_a":
            parent_index[edge["sub"]].append(edge["obj"])

    # Convert nodes to Terms
    terms: list[Term] = []
    for node in graph.get("nodes", []):
        if node.get("type") != "CLASS":
            continue
        if not node.get("lbl"):
            continue

        node_meta = node.get("meta", {})

        definition_obj = node_meta.get("definition")
        definition = definition_obj.get("val") if isinstance(definition_obj, dict) else None

        synonyms = [s["val"] for s in node_meta.get("synonyms", [])]
        deprecated = node_meta.get("deprecated", False)

        terms.append(
            Term(
                term_id=node["id"],
                label=node["lbl"],
                definition=definition,
                synonyms=synonyms,
                parent_ids=parent_index.get(node["id"], []),
                deprecated=deprecated,
            )
        )

    if not terms:
        raise ValueError("OBO Graphs data contains no CLASS nodes with labels")

    return ParsedOntology(
        title=title,
        description=description,
        version=version,
        terms=terms,
    )
