"""Unit tests for the ``Example`` and ``ConventionDocs`` value objects (#151, US2).

The VO encodes the mandatory-docs minimum itself: non-empty purpose, ≥1 worked
example, and ≥3 distinct trigger questions across
``example_questions ∪ {e.question for e in examples}`` — so an under-documented
deploy cannot construct a Convention at all (FR-013..016).
"""

import pytest
from pydantic import ValidationError

from osa.domain.deposition.model.docs import ConventionDocs, Example


def _example(question: str = "Which alloys stay ductile below -40C?") -> Example:
    return Example(
        question=question,
        query='POST /api/v1/data/alloy-tests/records {"filter": {}}',
        interpretation="Rows are individual test coupons.",
    )


def _docs(**overrides: object) -> ConventionDocs:
    kwargs: dict = {
        "purpose": "Test-campaign results for structural alloys.",
        "example_questions": [
            "Which alloys stay ductile below -40C?",
            "What is the yield strength range for 7xxx-series samples?",
            "Which samples have corrosion panels attached?",
        ],
        "examples": [_example()],
    }
    kwargs.update(overrides)
    return ConventionDocs.model_validate(kwargs)


class TestExample:
    def test_valid_example(self) -> None:
        ex = _example()
        assert ex.question.startswith("Which alloys")
        assert ex.query.startswith("POST ")
        assert ex.interpretation

    @pytest.mark.parametrize("field", ["question", "query", "interpretation"])
    @pytest.mark.parametrize("value", ["", "   "])
    def test_rejects_empty_fields(self, field: str, value: str) -> None:
        kwargs = {
            "question": "q?",
            "query": "GET /x",
            "interpretation": "means y",
        }
        kwargs[field] = value
        with pytest.raises(ValidationError):
            Example.model_validate(kwargs)


class TestConventionDocs:
    def test_valid_docs(self) -> None:
        docs = _docs()
        assert docs.purpose
        assert len(docs.examples) == 1
        assert docs.when_not_to_use is None
        assert docs.see_also is None

    def test_optional_fields_accepted(self) -> None:
        docs = _docs(
            when_not_to_use="Not a materials-property database.",
            see_also=["https://other-node.example.org"],
        )
        assert docs.when_not_to_use == "Not a materials-property database."
        assert docs.see_also == ["https://other-node.example.org"]

    @pytest.mark.parametrize("value", ["", "   "])
    def test_rejects_empty_purpose(self, value: str) -> None:
        with pytest.raises(ValidationError):
            _docs(purpose=value)

    def test_rejects_zero_examples(self) -> None:
        with pytest.raises(ValidationError):
            _docs(examples=[])

    def test_rejects_empty_example_question_entries(self) -> None:
        with pytest.raises(ValidationError):
            _docs(
                example_questions=["ok question?", "   ", "another?"],
            )

    def test_worked_example_questions_count_toward_three(self) -> None:
        # 2 trigger-only + 1 distinct worked-example question = 3 distinct.
        docs = _docs(
            example_questions=["q1?", "q2?"],
            examples=[_example("q3?")],
        )
        assert len(docs.example_questions) == 2

    def test_rejects_fewer_than_three_distinct_questions(self) -> None:
        with pytest.raises(ValidationError, match="3"):
            _docs(
                example_questions=["q1?"],
                examples=[_example("q2?")],
            )

    def test_duplicate_questions_count_once(self) -> None:
        # The worked example repeats a trigger question — only 2 distinct.
        with pytest.raises(ValidationError, match="3"):
            _docs(
                example_questions=["q1?", "q2?"],
                examples=[_example("q1?")],
            )

    def test_examples_alone_can_supply_the_breadth(self) -> None:
        docs = _docs(
            example_questions=[],
            examples=[_example("q1?"), _example("q2?"), _example("q3?")],
        )
        assert docs.example_questions == []

    def test_round_trips_through_serialization(self) -> None:
        docs = _docs(when_not_to_use="No.", see_also=["x"])
        restored = ConventionDocs.model_validate(docs.model_dump(mode="json"))
        assert restored == docs
