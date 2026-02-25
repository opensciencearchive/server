"""Shared source domain models used across deposition and source domains."""

from typing import Any

from pydantic import Field

from osa.domain.shared.model.value import ValueObject


class SourceLimits(ValueObject):
    """Resource limits for source container execution."""

    timeout_seconds: int = 3600
    memory: str = "4g"
    cpu: str = "2.0"


class SourceScheduleConfig(ValueObject):
    """Cron schedule for periodic source runs."""

    cron: str
    limit: int | None = None


class InitialRunConfig(ValueObject):
    """Configuration for the first source run on server startup."""

    limit: int | None = None


class SourceDefinition(ValueObject):
    """Complete specification for a source: image reference + config + limits."""

    image: str
    digest: str
    runner: str = "oci"
    config: dict[str, Any] | None = None
    limits: SourceLimits = Field(default_factory=SourceLimits)
    schedule: SourceScheduleConfig | None = None
    initial_run: InitialRunConfig | None = None
