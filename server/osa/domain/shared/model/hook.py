"""Shared hook domain models used across deposition and validation domains."""

from typing import Literal

from pydantic import Field

from osa.domain.shared.model.value import ValueObject


class ColumnDef(ValueObject):
    """Definition of a single column in a feature table."""

    name: str
    json_type: Literal["string", "number", "integer", "boolean", "array", "object"]
    format: str | None = None
    required: bool


class FeatureSchema(ValueObject):
    """Typed column definitions for features a hook produces."""

    columns: list[ColumnDef]


class HookManifest(ValueObject):
    """Manifest describing what a hook produces."""

    name: str
    record_schema: str
    cardinality: Literal["one", "many"]
    feature_schema: FeatureSchema
    runner: str = "oci"


class HookLimits(ValueObject):
    """Resource limits for hook execution."""

    timeout_seconds: int = 300
    memory: str = "2g"
    cpu: str = "2.0"


class HookDefinition(ValueObject):
    """Complete specification for a hook: image reference + manifest + limits."""

    image: str
    digest: str
    runner: str = "oci"
    config: dict | None = None
    limits: HookLimits = Field(default_factory=HookLimits)
    manifest: HookManifest
