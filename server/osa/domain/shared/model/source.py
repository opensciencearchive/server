"""Shared source domain models used across deposition and source domains."""

from typing import Annotated, Any, Literal, Union

from pydantic import Discriminator, Field, Tag, field_validator

from osa.domain.shared.model.value import ValueObject


class SourceLimits(ValueObject):
    """Resource limits for source container execution."""

    timeout_seconds: int = 3600
    memory: str = "512m"
    cpu: str = "0.25"


class SourceScheduleConfig(ValueObject):
    """Cron schedule for periodic source runs."""

    cron: str
    limit: int | None = None


class InitialRunConfig(ValueObject):
    """Configuration for the first source run on server startup."""

    limit: int | None = None


# ── RecordSource discriminated union ──


class _RecordSourceBase(ValueObject):
    """Base for all record source types."""

    type: str
    id: str

    @field_validator("id")
    @classmethod
    def id_must_be_non_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("id must be non-empty")
        return v


class DepositionSource(_RecordSourceBase):
    """Record originated from a user deposition."""

    type: Literal["deposition"] = "deposition"


class HarvestSource(_RecordSourceBase):
    """Record originated from an automated harvest run."""

    type: Literal["harvest"] = "harvest"
    harvest_run_srn: str
    upstream_source: str


def _record_source_discriminator(v: Any) -> str:
    if isinstance(v, dict):
        return v.get("type", "")
    return getattr(v, "type", "")


RecordSource = Annotated[
    Union[
        Annotated[DepositionSource, Tag("deposition")],
        Annotated[HarvestSource, Tag("harvest")],
    ],
    Discriminator(_record_source_discriminator),
]


# ── Source runner definitions ──


class SourceDefinition(ValueObject):
    """Complete specification for a source: image reference + config + limits."""

    image: str
    digest: str
    config: dict[str, Any] | None = None
    limits: SourceLimits = Field(default_factory=SourceLimits)
    schedule: SourceScheduleConfig | None = None
    initial_run: InitialRunConfig | None = None
