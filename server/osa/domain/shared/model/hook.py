"""Shared hook domain models used across deposition and validation domains.

A hook is conceptually two things — how it runs (RuntimeConfig) and what it
produces (FeatureSpec).  Both use discriminated unions so new variants
(NextflowConfig, TimeSeriesFeatureSpec, …) slot in without touching existing code.
"""

from typing import Annotated, Any, Literal

from pydantic import Field

from osa.domain.shared.model.value import ValueObject

# Lowercase alphanumeric + underscore, starting with a letter, max 63 chars.
# Safe for use as PG identifiers, file path components, and env var values.
PgIdentifier = Annotated[str, Field(pattern=r"^[a-z][a-z0-9_]{0,62}$")]


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
    memory: str = "2g"
    cpu: str = "2.0"


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
