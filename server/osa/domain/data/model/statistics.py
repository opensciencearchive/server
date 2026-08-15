"""Instance- and table-level statistics models for the data read surface.

The records/feature distinction is a *type*, not a nullable column:
``RecordsCount`` has no coverage (coverage of records by records is
definitionally the row count), ``FeatureCount`` always has it. The union makes
"records row with a coverage value" unrepresentable.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, Field


class InstanceStats(BaseModel):
    """Materialized instance-wide aggregates (storage, feature rows)."""

    storage_bytes: int
    feature_rows: int
    computed_at: datetime


class RecordsCount(BaseModel):
    """Counts for a schema version's records table."""

    kind: Literal["records"] = "records"
    row_count: int = 0


class FeatureCount(BaseModel):
    """Counts for one feature table scoped to a schema version.

    ``records_covered`` = how many of the schema's records have ≥1 row here.
    """

    kind: Literal["feature"] = "feature"
    row_count: int = 0
    records_covered: int = 0


TableCount = Annotated[RecordsCount | FeatureCount, Field(discriminator="kind")]


class SchemaTableCounts(BaseModel):
    """Lockstep counts for one schema version — the manifest's count source.

    Absent state is zero by construction: a fresh schema yields default
    ``RecordsCount()`` / ``FeatureCount()`` instances, never an error.
    """

    records: RecordsCount = Field(default_factory=RecordsCount)
    features: dict[str, FeatureCount] = Field(default_factory=dict)

    def feature(self, name: str) -> FeatureCount:
        return self.features.get(name, FeatureCount())


class TableCountEntry(BaseModel):
    """One table's counts with its full identity — stored or recomputed truth."""

    schema_id: str
    schema_version: str
    table_name: str
    counts: TableCount


class StatisticsDrift(BaseModel):
    """A stored count that disagrees with the recomputed truth.

    ``None`` on either side means the row is absent there: ``stored=None`` is a
    table the stats missed; ``actual=None`` is an orphan stats row whose data
    is gone.
    """

    schema_id: str
    schema_version: str
    table_name: str
    stored: TableCount | None
    actual: TableCount | None
