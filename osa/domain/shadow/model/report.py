from datetime import datetime
from typing import Any

from pydantic import BaseModel

from osa.domain.shadow.model.aggregate import ShadowId


class ShadowReport(BaseModel):
    shadow_id: ShadowId
    source_domain: str
    validation_summary: dict[str, Any]
    score: str
    created_at: datetime
