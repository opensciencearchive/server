"""FieldDefinition.examples — optional author-supplied representative values (#151)."""

from osa.domain.semantics.model.value import Cardinality, FieldDefinition, FieldType


def _field(**overrides: object) -> FieldDefinition:
    kwargs: dict = {
        "name": "yield_strength",
        "type": FieldType.NUMBER,
        "required": True,
        "cardinality": Cardinality.EXACTLY_ONE,
    }
    kwargs.update(overrides)
    return FieldDefinition.model_validate(kwargs)


class TestFieldDefinitionExamples:
    def test_examples_defaults_to_none(self) -> None:
        field = _field()
        assert field.examples is None

    def test_examples_accepts_list_of_strings(self) -> None:
        field = _field(examples=["512", "480"])
        assert field.examples == ["512", "480"]

    def test_examples_round_trips_through_serialization(self) -> None:
        field = _field(examples=["512"])
        restored = FieldDefinition.model_validate(field.model_dump())
        assert restored == field
        assert restored.examples == ["512"]

    def test_absent_examples_round_trips_unchanged(self) -> None:
        field = _field()
        restored = FieldDefinition.model_validate(field.model_dump())
        assert restored == field
        assert restored.examples is None
