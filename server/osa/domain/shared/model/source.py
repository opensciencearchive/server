"""Shared source domain models used across deposition and ingest domains."""

from typing import Annotated, Any, ClassVar, Literal, Union

from pydantic import Discriminator, Field, Tag, field_validator

from osa.domain.shared.model.names import PgName
from osa.domain.shared.model.value import ValueObject


class IngesterName(PgName):
    """An ingester's stable name (#180).

    Lives in the shared kernel because the deploy path (deposition domain)
    registers ingesters that the ingest domain later runs.
    """

    kind: ClassVar[str] = "ingester name"


class IngesterLimits(ValueObject):
    """Resource limits for ingester container execution."""

    timeout_seconds: int = 3600
    memory: str = "1g"
    cpu: str = "0.25"


class IngesterScheduleConfig(ValueObject):
    """Cron schedule for periodic ingester runs."""

    cron: str
    limit: int | None = None


class InitialRunConfig(ValueObject):
    """Configuration for the first ingester run on server startup."""

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


class IngestSource(_RecordSourceBase):
    """Record originated from an automated ingest run."""

    type: Literal["ingest"] = "ingest"
    ingest_run_id: str
    upstream_source: str
    batch_index: int | None = None
    """Which ingest batch published this record (#160).

    ``None`` only for records predating the field.
    """


def _record_source_discriminator(v: Any) -> str:
    if isinstance(v, dict):
        return v.get("type", "")
    return getattr(v, "type", "")


RecordSource = Annotated[
    Union[
        Annotated[DepositionSource, Tag("deposition")],
        Annotated[IngestSource, Tag("ingest")],
    ],
    Discriminator(_record_source_discriminator),
]


# ── Ingester runner definitions ──


class IngesterDefinition(ValueObject):
    """Complete specification for an ingester: identity + image + config + limits.

    ``name`` is the ingester's registry identity (#180 §1) — a declared ingester
    IS an identity, so a nameless one is unrepresentable. ``source_ref`` is the
    reproducibility anchor for the build that produced ``image`` (parity with a
    hook release's ``source_ref``); a release without provenance is equally
    unrepresentable.
    """

    name: IngesterName
    image: str
    digest: str
    config: dict[str, Any] | None = None
    limits: IngesterLimits = Field(default_factory=IngesterLimits)
    schedule: IngesterScheduleConfig | None = None
    initial_run: InitialRunConfig | None = None
    source_ref: str
