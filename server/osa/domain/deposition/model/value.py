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


class SubmissionStage(StrEnum):
    """Progress checkpoint for the submission workflow (#160).

    Ordered: SUBMITTED < VALIDATED < PUBLISHED. On retry the workflow
    re-enters from the top and skips stages the checkpoint has passed.

    Ordering is by member *definition position*, not by string value. StrEnum
    inherits str's lexicographic comparisons (under which ``"published" <
    "submitted"``), so all four operators are defined explicitly — a
    ``@total_ordering`` decorator would see str's operators as already present
    and fill nothing.
    """

    SUBMITTED = "submitted"
    VALIDATED = "validated"
    PUBLISHED = "published"

    def __lt__(self, other: object) -> bool:
        """Order by member definition position, not by string value."""
        if not isinstance(other, SubmissionStage):
            return NotImplemented
        order = list(SubmissionStage)
        return order.index(self) < order.index(other)

    def __le__(self, other: object) -> bool:
        if not isinstance(other, SubmissionStage):
            return NotImplemented
        return self == other or self < other

    def __gt__(self, other: object) -> bool:
        if not isinstance(other, SubmissionStage):
            return NotImplemented
        return not self <= other

    def __ge__(self, other: object) -> bool:
        if not isinstance(other, SubmissionStage):
            return NotImplemented
        return not self < other


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
