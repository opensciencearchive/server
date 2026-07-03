"""Read-side DTOs for the skill surface (#151).

These are projections consumed by the skill generator/renderer. ``AuthorDocs``
mirrors the deposition-owned ``ConventionDocs`` shape but is a distinct read
DTO — the ``data`` domain never imports ``deposition`` internals; the read
store validates the persisted ``conventions.docs`` JSONB into this shape.
"""

from __future__ import annotations

from pydantic import BaseModel


class NodeIdentity(BaseModel):
    """Node identity block of the root discovery document (from ``Config``)."""

    name: str
    domain: str
    description: str
    osa_version: str


class RootDiscovery(BaseModel):
    """The ``GET /`` response body (contracts/root-discovery.md)."""

    node: NodeIdentity
    skill_url: str
    reference_base: str
    data_url: str
    openapi_url: str
    schemas: list[str]  # "<id>@<version>" at latest published version


class ExampleDoc(BaseModel):
    """A worked example, rendered verbatim (FR-018)."""

    question: str
    query: str
    interpretation: str


class AuthorDocs(BaseModel):
    """Author semantics for one schema, projected from its owning convention.

    ``examples`` is required — the persisted ``ConventionDocs`` guarantees ≥1
    worked example, and this read DTO must not re-admit the state the write
    side made unrepresentable.
    """

    purpose: str
    example_questions: list[str] = []
    examples: list[ExampleDoc]
    when_not_to_use: str | None = None
    see_also: list[str] | None = None

    def trigger_questions(self) -> list[str]:
        """Distinct trigger-question union, in first-seen order (FR-002)."""
        seen: dict[str, None] = {}
        for q in [*self.example_questions, *(e.question for e in self.examples)]:
            seen.setdefault(q.strip())
        return list(seen)


class SampleValue(BaseModel):
    """One sampled non-null value for example templating (research §9)."""

    value: str | int | float | bool


class DatasetEntry(BaseModel):
    """One row of the SKILL.md datasets table."""

    schema_id: str  # bare short id, e.g. "alloy-tests" (reference link target)
    schema_ref: str  # "<id>@<version>"
    title: str
    row_count: int
