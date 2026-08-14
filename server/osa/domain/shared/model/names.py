"""Nominal name types for things that compose into PostgreSQL identifiers.

Hooks, features and ingesters all name the same *kind* of thing — a stable,
lowercase identifier that ends up inside a PG object name — and all three had
independently copied the same regex, validator and ``__str__``. :class:`PgName`
holds that shape once; each subclass exists purely so the type checker can tell
a hook name from a feature name from an ingester name.

The 40-character cap is not arbitrary. These names compose into PG identifiers
alongside fixed prefixes and suffixes — the widest being the per-hook FK
constraint ``fk_features_{name}_record_srn`` at 23 characters of overhead.
PG's identifier limit is 63, so capping names at 40 keeps every derived
identifier inside the limit without surprise truncation.
"""

from __future__ import annotations

import re
from typing import ClassVar, Self

from pydantic import ConfigDict, RootModel, field_validator

MAX_NAME_LENGTH = 40

_NAME_PATTERN = re.compile(rf"^[a-z][a-z0-9_]{{0,{MAX_NAME_LENGTH - 1}}}$")


class PgName(RootModel[str]):
    """A stable name that is safe to compose into a PG identifier.

    Frozen, so instances are hashable and usable as dict keys
    (``dict[HookName, ...]``). Use ``.root`` where a plain ``str`` is required —
    PG identifiers built without interpolation, dict keys handed to infra.
    """

    model_config = ConfigDict(frozen=True)

    #: Names the kind of thing in validation errors ("hook name", "feature name").
    kind: ClassVar[str] = "name"

    @field_validator("root")
    @classmethod
    def _validate(cls, value: str) -> str:
        if not _NAME_PATTERN.match(value):
            raise ValueError(
                f"invalid {cls.kind}: 1–{MAX_NAME_LENGTH} chars of [a-z0-9_], "
                "starting with a letter"
            )
        return value

    def __str__(self) -> str:
        return self.root

    @classmethod
    def from_raw(cls, raw: str) -> Self:
        """Parse a free-form name into a canonical PgName at the boundary.

        Wire names arrive as Python class names (``PDBIngester``) or declared
        slugs (``usgs-feed``). Normalisation is deterministic — lowercase,
        non-``[a-z0-9]`` runs collapse to a single ``_``, truncated to the cap —
        so the same wire name always maps to the same registry identity.
        Raises ``ValueError`` when nothing valid remains (e.g. all digits).
        """
        # Break CamelCase before lowering so PDBFeed → pdb_feed, not pdbfeed:
        # lower/digit→Upper boundaries, then acronym→word (USGSFeed → USGS_Feed).
        decamel = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", raw)
        decamel = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", "_", decamel)
        normalised = re.sub(r"[^a-z0-9]+", "_", decamel.lower()).strip("_")
        return cls(normalised[:MAX_NAME_LENGTH].rstrip("_"))
