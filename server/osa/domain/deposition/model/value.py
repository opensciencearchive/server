from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, Field

from osa.domain.shared.model.value import ValueObject


class DepositionStatus(StrEnum):
    DRAFT = "draft"
    IN_VALIDATION = "in_validation"
    IN_REVIEW = "in_review"
    ACCEPTED = "accepted"
    REJECTED = "rejected"


class DepositionFile(BaseModel):
    name: str
    size: int
    checksum: str
    content_type: str | None = None
    uploaded_at: datetime = Field(default_factory=datetime.now)


class FileRequirements(ValueObject):
    """File upload constraints for a convention."""

    accepted_types: list[str]  # e.g., [".csv", ".h5ad"]
    min_count: int = 0
    max_count: int
    max_file_size: int  # bytes
