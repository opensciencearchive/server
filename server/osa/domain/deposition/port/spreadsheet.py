from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Protocol

from pydantic import BaseModel

from osa.domain.shared.port import Port

if TYPE_CHECKING:
    from osa.domain.semantics.model.schema import Schema


class SpreadsheetError(BaseModel):
    """A single field-level error from spreadsheet parsing."""

    field: str
    message: str


class SpreadsheetParseResult(BaseModel):
    """Result of parsing a spreadsheet upload."""

    metadata: dict[str, Any]
    warnings: list[str] = []
    errors: list[SpreadsheetError] = []


class SpreadsheetPort(Port, Protocol):
    @abstractmethod
    def generate_template(
        self,
        schema: "Schema",
        ontology_terms_by_srn: dict[str, list[str]],
    ) -> bytes: ...

    @abstractmethod
    def parse_upload(
        self,
        schema: "Schema",
        content: bytes,
    ) -> SpreadsheetParseResult: ...
