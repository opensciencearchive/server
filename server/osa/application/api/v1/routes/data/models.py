"""Shared Pydantic response models for the ``/data/`` routes."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel

from osa.domain.data.model.record_summary import RecordSummary


class RecordResponse(BaseModel):
    """Single-record response — carries BOTH the bare ``id`` and full ``srn``."""

    id: str
    srn: str
    schema_id: str
    version: int
    metadata: dict[str, Any]
    created_at: datetime

    @classmethod
    def from_summary(cls, summary: RecordSummary) -> "RecordResponse":
        return cls(
            id=str(summary.id),
            srn=str(summary.srn),
            schema_id=summary.schema_id.render(),
            version=summary.version,
            metadata=summary.metadata,
            created_at=summary.created_at,
        )
