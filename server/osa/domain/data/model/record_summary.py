"""Row types yielded by the read engine.

``RecordSummary`` is the records-table row. Per data-model.md every records
response carries BOTH the bare internal ``id`` and the full ``srn``; ``id`` and
``version`` are derivable from the SRN, ``schema_id`` and ``created_at`` from
the ``records`` table columns. Feature-table rows do not have SRNs in v1 — they
flow through the engine as plain column→value mappings, not ``RecordSummary``.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel

from osa.domain.shared.model.ids import RecordId
from osa.domain.shared.model.srn import RecordSRN, SchemaId

# Implicit columns present on every records-table response, in wire order,
# ahead of the schema's declared metadata fields.
IMPLICIT_RECORD_COLUMNS: tuple[str, ...] = ("id", "srn", "schema_id", "version", "created_at")


class RecordSummary(BaseModel):
    """A single published record as projected by the read engine."""

    id: RecordId
    srn: RecordSRN
    schema_id: SchemaId
    version: int
    metadata: dict[str, Any]
    created_at: datetime

    def flatten(self) -> dict[str, Any]:
        """Flatten into a column→value mapping for serialization.

        Implicit columns come first (in :data:`IMPLICIT_RECORD_COLUMNS` order),
        then the metadata fields. Metadata keys never shadow implicit columns
        because schema field names cannot collide with the reserved implicit
        set at registration time.
        """
        return {
            "id": str(self.id),
            "srn": str(self.srn),
            "schema_id": self.schema_id.render(),
            "version": self.version,
            "created_at": self.created_at.isoformat(),
            **self.metadata,
        }
