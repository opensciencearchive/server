"""Shared hook domain models used across deposition and validation domains.

A hook is conceptually two things — how it runs (RuntimeConfig) and what it
produces (FeatureSpec).  Both use discriminated unions so new variants
(NextflowConfig, TimeSeriesFeatureSpec, …) slot in without touching existing code.
"""

import re
from typing import Annotated, Any, ClassVar, Literal

from pydantic import Field

from osa.domain.shared.model.names import PgName
from osa.domain.shared.model.value import ValueObject

# Lowercase alphanumeric + underscore, starting with a letter, max 63 chars.
# Safe for use as PG identifiers, file path components, and env var values.
PgIdentifier = Annotated[str, Field(pattern=r"^[a-z][a-z0-9_]{0,62}$")]


class HookName(PgName):
    """A hook's stable name (#145).

    A nominal type so the type checker can distinguish a hook name from any
    other string and the regex is enforced at construction. Column names use
    plain :data:`PgIdentifier` instead, because they don't get composed into
    longer identifiers.
    """

    kind: ClassVar[str] = "hook name"


class FeatureName(PgName):
    """Identity of a feature table on the read surface (#145).

    A hook produces exactly one feature table, addressed at
    ``/data/{schema}/{feature}``; its name equals the producing hook's name but
    is a **distinct nominal type** so the read/feature side never traffics in
    "hook". Convert a producing hook's name once at the hook→feature boundary
    (``FeatureName(hook.name.root)``); reserved-slot names are already rejected
    upstream on the hook identity.
    """

    kind: ClassVar[str] = "feature name"


_MEMORY_RE = re.compile(r"^(\d+(?:\.\d+)?)(g|m|k)?i?$")

_GIB = 1024 * 1024 * 1024
_MIB = 1024 * 1024
_KIB = 1024


def parse_memory(memory: str) -> int:
    """Parse memory string like '2g' or '512m' to bytes."""
    match = _MEMORY_RE.match(memory.strip().lower())
    if not match:
        raise ValueError(f"Invalid memory format: {memory}")

    amount = float(match.group(1))
    unit = match.group(2)

    match unit:
        case "g":
            return int(amount * _GIB)
        case "m":
            return int(amount * _MIB)
        case "k":
            return int(amount * _KIB)
        case None:
            return int(amount)
        case _:
            raise ValueError(f"Unknown memory unit: {unit}")


def format_memory(byte_count: int) -> str:
    """Format bytes to a compact memory string (e.g. '2g', '1536m')."""
    if byte_count % _GIB == 0:
        return f"{byte_count // _GIB}g"
    if byte_count % _MIB == 0:
        return f"{byte_count // _MIB}m"
    if byte_count % _KIB == 0:
        return f"{byte_count // _KIB}k"
    return str(byte_count)


class ColumnDef(ValueObject):
    """Definition of a single column in a feature or metadata table."""

    name: PgIdentifier
    json_type: Literal["string", "number", "integer", "boolean", "array", "object"]
    format: str | None = None
    required: bool
    description: str | None = None
    unit: str | None = None


# ── Runtime variants ──


class OciLimits(ValueObject):
    """Resource limits for OCI hook execution."""

    timeout_seconds: int = 300
    memory: str = "1g"
    cpu: str = "0.5"


class RuntimeConfig(ValueObject):
    """Base for runtime configuration.  Discriminated on ``type``."""

    type: str


class OciConfig(RuntimeConfig):
    """OCI container runtime configuration."""

    type: Literal["oci"] = "oci"
    image: str
    digest: str
    config: dict[str, Any] = Field(default_factory=dict)
    limits: OciLimits = Field(default_factory=OciLimits)


# ── Feature variants ──


class FeatureSpec(ValueObject):
    """Base for feature specifications.  Discriminated on ``kind``."""

    kind: str


class TableFeatureSpec(FeatureSpec):
    """Table-shaped feature output with typed columns."""

    kind: Literal["table"] = "table"
    cardinality: Literal["one", "many"]
    columns: list[ColumnDef]


# ── Hook ──


class HookIdentity(ValueObject):
    """A hook's stable **identity**: its name + the output contract it produces.

    Feature #145 split the old monolithic hook spec into two: this identity
    (``name`` + ``feature``, fixed forever and owning the feature table) and the
    versioned, immutable ``HookRelease`` entity (``runtime`` + ``source_ref``)
    in the ``validation`` domain that says *what image* currently computes that
    output. This value object is the identity shape used by the bundled deploy
    input and the ``ConventionRegistered`` event; its persisted form is the
    ``Hook`` aggregate.
    """

    name: HookName
    feature: Annotated[TableFeatureSpec, Field(discriminator="kind")]

    def model_post_init(self, __context: object) -> None:
        # A hook name becomes a feature-table URL slot under /data/{schema}/.
        # Reject names that would shadow a fixed slot (research §6).
        from osa.domain.shared.error import ReservedNameError
        from osa.domain.shared.model.reserved import RESERVED_NAMES

        if self.name.root in RESERVED_NAMES:
            raise ReservedNameError(self.name.root, "hook")
