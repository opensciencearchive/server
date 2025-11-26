from enum import StrEnum

from osa.domain.shared.model.value import RootValueObject


class ValidationStatus(StrEnum):
    PASS = "pass"
    FAIL = "fail"


class ValidatorMessage(RootValueObject[str]): ...
