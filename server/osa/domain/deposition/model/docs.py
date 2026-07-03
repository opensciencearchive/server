"""Author-supplied convention documentation (#151).

``ConventionDocs`` is the semantics block attached to every Convention at
deploy: what the dataset is for, the questions it answers, and worked
examples rendered verbatim into the reference doc (FR-018). The mandatory
minimum — non-empty purpose, ≥1 worked example, ≥3 distinct trigger questions
across ``example_questions ∪ {e.question for e in examples}`` — is encoded in
the model itself, so an under-documented deploy cannot construct a Convention
at all (FR-013..016).
"""

from typing import Annotated

from pydantic import Field, field_validator, model_validator

from osa.domain.shared.model.value import ValueObject

NonEmptyStr = Annotated[str, Field(min_length=1)]

MIN_DISTINCT_TRIGGER_QUESTIONS = 3


def _require_stripped(value: str, field_name: str) -> str:
    if not value.strip():
        raise ValueError(f"{field_name} must not be empty or whitespace-only")
    return value


class Example(ValueObject):
    """A worked example: question, opaque query, and what the answer means.

    ``query`` is never parsed, executed, or validated (FR-011) — it is rendered
    verbatim into the reference doc.
    """

    question: NonEmptyStr
    query: NonEmptyStr
    interpretation: NonEmptyStr

    @field_validator("question", "query", "interpretation")
    @classmethod
    def _non_blank(cls, v: str, info) -> str:
        return _require_stripped(v, info.field_name)


class ConventionDocs(ValueObject):
    """The author-semantics block attached to a Convention at deploy."""

    purpose: NonEmptyStr
    # Trigger-only questions (no worked answer); may be empty when the worked
    # examples alone supply the required breadth.
    example_questions: list[NonEmptyStr] = []
    examples: list[Example] = Field(min_length=1)
    when_not_to_use: str | None = None
    see_also: list[str] | None = None

    @field_validator("purpose")
    @classmethod
    def _purpose_non_blank(cls, v: str) -> str:
        return _require_stripped(v, "purpose")

    @field_validator("example_questions")
    @classmethod
    def _questions_non_blank(cls, v: list[str]) -> list[str]:
        for q in v:
            _require_stripped(q, "example_questions entries")
        return v

    @model_validator(mode="after")
    def _require_trigger_breadth(self) -> "ConventionDocs":
        distinct = {q.strip() for q in self.example_questions}
        distinct |= {e.question.strip() for e in self.examples}
        if len(distinct) < MIN_DISTINCT_TRIGGER_QUESTIONS:
            raise ValueError(
                f"need at least {MIN_DISTINCT_TRIGGER_QUESTIONS} distinct trigger "
                f"questions across example_questions and worked-example questions, "
                f"got {len(distinct)}"
            )
        return self

    def trigger_questions(self) -> list[str]:
        """The distinct trigger-question union, in first-seen order.

        Feeds the skill frontmatter's trigger prose (FR-002).
        """
        seen: dict[str, None] = {}
        for q in [*self.example_questions, *(e.question for e in self.examples)]:
            seen.setdefault(q.strip())
        return list(seen)
