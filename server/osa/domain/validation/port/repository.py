from typing import Protocol, runtime_checkable

from osa.domain.shared.model.srn import ValidationRunSRN
from osa.domain.shared.port import Port
from osa.domain.validation.model import ValidationRun


@runtime_checkable
class ValidationRunRepository(Port, Protocol):
    """Store validation run records."""

    async def get(self, srn: ValidationRunSRN) -> ValidationRun | None: ...

    async def save(self, run: ValidationRun) -> None: ...
