from typing import Protocol, runtime_checkable

from osa.domain.shared.model.srn import TraitSRN, ValidationRunSRN
from osa.domain.shared.port import Port
from osa.domain.validation.model import Trait, ValidationRun


@runtime_checkable
class TraitRepository(Port, Protocol):
    """Store and resolve traits by SRN."""

    async def get(self, srn: TraitSRN) -> Trait | None: ...

    async def get_or_fetch(self, srn: TraitSRN) -> Trait:
        """Get locally or fetch from remote node. Raises if not found."""
        ...

    async def save(self, trait: Trait) -> None: ...

    async def list(self) -> list[Trait]: ...


@runtime_checkable
class ValidationRunRepository(Port, Protocol):
    """Store validation run records."""

    async def get(self, srn: ValidationRunSRN) -> ValidationRun | None: ...

    async def save(self, run: ValidationRun) -> None: ...
