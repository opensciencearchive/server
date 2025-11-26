

from enum import StrEnum


class DepositionStatus(StrEnum):
    DRAFT = "draft"
    SUBMITTED = "submitted"
    IN_REVIEW = "in review"
    ACCEPTED = "accepted"
    REJECTED = "rejected"
