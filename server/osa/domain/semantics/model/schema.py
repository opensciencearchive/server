from datetime import datetime

from osa.domain.semantics.model.value import FieldDefinition
from osa.domain.shared.error import ReservedNameError, ValidationError
from osa.domain.shared.model.aggregate import Aggregate
from osa.domain.shared.model.reserved import RESERVED_NAMES
from osa.domain.shared.model.srn import SchemaId


class Schema(Aggregate):
    """An immutable, versioned definition of metadata structure."""

    id: SchemaId
    title: str
    fields: list[FieldDefinition]
    created_at: datetime

    def model_post_init(self, __context: object) -> None:
        if self.id.id.root in RESERVED_NAMES:
            raise ReservedNameError(self.id.id.root, "schema")

        if len(self.fields) < 1:
            raise ValidationError("Schema must have at least one field")

        names = [f.name for f in self.fields]
        if len(names) != len(set(names)):
            raise ValidationError("Duplicate field names within schema")
