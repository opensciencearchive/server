from datetime import datetime

from osa.domain.semantics.model.value import FieldDefinition
from osa.domain.shared.error import ValidationError
from osa.domain.shared.model.aggregate import Aggregate
from osa.domain.shared.model.srn import SchemaSRN


class Schema(Aggregate):
    """An immutable, versioned definition of metadata structure."""

    srn: SchemaSRN
    title: str
    fields: list[FieldDefinition]
    created_at: datetime

    def model_post_init(self, __context: object) -> None:
        if len(self.fields) < 1:
            raise ValidationError("Schema must have at least one field")

        names = [f.name for f in self.fields]
        if len(names) != len(set(names)):
            raise ValidationError("Duplicate field names within schema")
