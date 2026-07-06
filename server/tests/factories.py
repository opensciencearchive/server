"""Shared test factories for cross-cutting model construction."""

from typing import Any

from osa.domain.deposition.model.docs import ConventionDocs, Example


def make_convention_docs(**overrides: Any) -> ConventionDocs:
    """A minimal valid ``ConventionDocs`` — every convention must carry docs (#151)."""
    kwargs: dict[str, Any] = {
        "purpose": "Test-campaign data for unit tests.",
        "example_questions": [
            "What records exist?",
            "Which samples pass validation?",
            "What is the value range for field X?",
        ],
        "examples": [
            Example(
                question="What records exist?",
                query="GET /api/v1/data/test/records",
                interpretation="One row per deposited record.",
            )
        ],
    }
    kwargs.update(overrides)
    return ConventionDocs(**kwargs)


def make_convention_docs_dict(**overrides: Any) -> dict[str, Any]:
    """The same minimal docs block as a JSON-ready dict (for raw row inserts)."""
    return make_convention_docs(**overrides).model_dump(mode="json")
