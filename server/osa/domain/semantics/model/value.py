from enum import StrEnum
from typing import Annotated, Literal, Union

from pydantic import Field

from osa.domain.shared.model.srn import OntologySRN
from osa.domain.shared.model.value import ValueObject


class FieldType(StrEnum):
    TEXT = "text"
    NUMBER = "number"
    DATE = "date"
    BOOLEAN = "boolean"
    TERM = "term"
    URL = "url"


class Cardinality(StrEnum):
    EXACTLY_ONE = "exactly_one"
    ONE_OR_MORE = "one_or_more"
    ZERO_OR_MORE = "zero_or_more"


class TextConstraints(ValueObject):
    type: Literal["text"] = "text"
    min_length: int | None = None
    max_length: int | None = None
    pattern: str | None = None


class NumberConstraints(ValueObject):
    type: Literal["number"] = "number"
    min_value: float | None = None
    max_value: float | None = None
    integer_only: bool = False
    unit: str | None = None


class TermConstraints(ValueObject):
    type: Literal["term"] = "term"
    ontology_srn: OntologySRN
    root_term: str | None = None


class UrlConstraints(ValueObject):
    type: Literal["url"] = "url"
    pattern: str | None = None


class DateConstraints(ValueObject):
    type: Literal["date"] = "date"


class BooleanConstraints(ValueObject):
    type: Literal["boolean"] = "boolean"


FieldConstraints = Annotated[
    Union[
        TextConstraints,
        NumberConstraints,
        TermConstraints,
        UrlConstraints,
        DateConstraints,
        BooleanConstraints,
    ],
    Field(discriminator="type"),
]


class FieldDefinition(ValueObject):
    """A single field definition within a schema."""

    name: str
    type: FieldType
    required: bool
    cardinality: Cardinality
    description: str | None = None
    constraints: FieldConstraints | None = None
