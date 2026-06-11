"""Central semantic ID types used across the ``/data/`` read surface.

Per OSA's type-safety convention, semantic identifiers cross module
boundaries as ``NewType`` aliases rather than bare ``str``. ``HookName`` is
re-exported from :mod:`osa.domain.shared.model.hook` (its source of truth,
where the PG-identifier pattern validation lives) so callers have a single
import location for the IDs this surface deals in.
"""

from __future__ import annotations

from typing import NewType

from osa.domain.shared.error import ValidationError
from osa.domain.shared.model.hook import HookName
from osa.domain.shared.model.value import ValueObject

# Bare internal record identifier (UUIDv7 / ULID). Validation of the exact
# charset/length happens at the API boundary; internally it is opaque.
RecordId = NewType("RecordId", str)


class RecordRef(ValueObject):
    """A record reference: bare internal id plus optional integer version.

    Wire form is the URL segment ``{id}`` or ``{id}@{version}`` — the record
    analogue of :class:`~osa.domain.shared.model.srn.SchemaId`'s
    ``<id>@<semver>``. ``version is None`` means "latest published".
    """

    id: RecordId
    version: int | None = None

    @classmethod
    def parse(cls, raw: str) -> RecordRef:
        """Parse ``{id}`` or ``{id}@{version}``; raises ``ValidationError``."""
        if "@" not in raw:
            return cls(id=RecordId(raw))
        id_part, version_part = raw.split("@", 1)
        try:
            return cls(id=RecordId(id_part), version=int(version_part))
        except ValueError as exc:
            raise ValidationError(
                f"Invalid record version in {raw!r}; expected an integer.",
                field="id",
            ) from exc

    def render(self) -> str:
        return self.id if self.version is None else f"{self.id}@{self.version}"

    def __str__(self) -> str:
        return self.render()


__all__ = ["RecordId", "RecordRef", "HookName"]
