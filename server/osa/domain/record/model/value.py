"""Record domain value objects."""

from datetime import datetime

from osa.domain.shared.model.value import ValueObject


class IndexRef(ValueObject):
    """A pointer to a record's data in an external index.

    Each index adapter knows how to use the external_id to retrieve data.
    The Record stores a dict mapping index_id -> IndexRef.

    Example:
        record.indexes = {
            "vector": IndexRef(external_id="abc123", indexed_at=...),
            "files": IndexRef(external_id="s3://bucket/prefix/", indexed_at=...),
        }
    """

    external_id: str  # The ID/path used to find this record's data in the index
    indexed_at: datetime | None = None
