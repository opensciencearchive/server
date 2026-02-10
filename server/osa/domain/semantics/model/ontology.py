from datetime import datetime

from pydantic import BaseModel

from osa.domain.shared.error import ValidationError
from osa.domain.shared.model.aggregate import Aggregate
from osa.domain.shared.model.srn import OntologySRN


class Term(BaseModel):
    """An individual entry within an ontology."""

    term_id: str
    label: str
    synonyms: list[str] = []
    parent_ids: list[str] = []
    definition: str | None = None
    deprecated: bool = False


class Ontology(Aggregate):
    """An immutable, versioned collection of terms."""

    srn: OntologySRN
    title: str
    description: str | None = None
    terms: list[Term]
    created_at: datetime

    def model_post_init(self, __context: object) -> None:
        if len(self.terms) < 1:
            raise ValidationError("Ontology must have at least one term")

        term_ids = [t.term_id for t in self.terms]
        if len(term_ids) != len(set(term_ids)):
            raise ValidationError("Duplicate term IDs within ontology")
