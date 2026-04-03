"""Shared hook domain models used across deposition and validation domains.

A hook is conceptually two things — how it runs (RuntimeConfig) and what it
produces (FeatureSpec).  Both use discriminated unions so new variants
(NextflowConfig, TimeSeriesFeatureSpec, …) slot in without touching existing code.
"""

import re
from typing import Annotated, Any, Literal

from pydantic import Field

from osa.domain.shared.model.value import ValueObject

# Lowercase alphanumeric + underscore, starting with a letter, max 63 chars.
# Safe for use as PG identifiers, file path components, and env var values.
PgIdentifier = Annotated[str, Field(pattern=r"^[a-z][a-z0-9_]{0,62}$")]

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


def _format_memory(byte_count: int) -> str:
    """Format bytes to a compact memory string (e.g. '2g', '1536m')."""
    if byte_count % _GIB == 0:
        return f"{byte_count // _GIB}g"
    if byte_count % _MIB == 0:
        return f"{byte_count // _MIB}m"
    if byte_count % _KIB == 0:
        return f"{byte_count // _KIB}k"
    return str(byte_count)


class ColumnDef(ValueObject):
    """Definition of a single column in a feature table."""

    name: PgIdentifier
    json_type: Literal["string", "number", "integer", "boolean", "array", "object"]
    format: str | None = None
    required: bool


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


class HookDefinition(ValueObject):
    """Complete specification for a hook: how it runs + what it produces."""

    name: PgIdentifier
    runtime: Annotated[OciConfig, Field(discriminator="type")]
    feature: Annotated[TableFeatureSpec, Field(discriminator="kind")]

    def with_memory(self, memory: str) -> "HookDefinition":
        """Return a copy with a different memory limit."""
        new_limits = self.runtime.limits.model_copy(update={"memory": memory})
        new_runtime = self.runtime.model_copy(update={"limits": new_limits})
        return self.model_copy(update={"runtime": new_runtime})

    def with_doubled_memory(self) -> "HookDefinition":
        """Return a copy with 2x the current memory limit."""
        current_bytes = parse_memory(self.runtime.limits.memory)
        doubled = _format_memory(current_bytes * 2)
        return self.with_memory(doubled)
