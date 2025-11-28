from enum import StrEnum


class ShadowStatus(StrEnum):
    PENDING = "pending"
    INGESTING = "ingesting"
    VALIDATING = "validating"
    COMPLETED = "completed"
    FAILED = "failed"
