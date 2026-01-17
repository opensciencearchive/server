from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, Field


class DepositionStatus(StrEnum):
    DRAFT = "draft"
    SUBMITTED = "submitted"
    IN_REVIEW = "in review"
    ACCEPTED = "accepted"
    REJECTED = "rejected"


class DepositionFile(BaseModel):
    name: str
    size: int
    checksum: str
    uploaded_at: datetime = Field(default_factory=datetime.now)
