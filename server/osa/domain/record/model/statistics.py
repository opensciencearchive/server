"""Instance-wide statistics — the materialized snapshot of O(rows) aggregates."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class InstanceStats(BaseModel):
    """Precomputed instance-wide aggregates.

    Only the expensive-to-compute figures are materialized here (total storage
    footprint and total feature-table rows); cheap counts (records, this-month)
    are read live. Refreshed periodically by the WorkerPool.
    """

    storage_bytes: int
    feature_rows: int
    computed_at: datetime
