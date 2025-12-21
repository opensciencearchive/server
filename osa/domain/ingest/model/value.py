"""Ingest configuration value objects."""

from datetime import datetime
from typing import Annotated, Union

from pydantic import BaseModel, Field

from osa.infrastructure.ingest.geo.config import GEOIngestorConfig

# Union of all ingestor configs (extend as new ingestors are added)
AnyIngestorConfig = Annotated[
    Union[GEOIngestorConfig],
    Field(discriminator=None),
]


class IngestSchedule(BaseModel):
    """Schedule configuration for an ingestor."""

    cron: str  # Cron expression (e.g., "0 * * * *" for hourly)
    limit: int | None = None  # Optional limit per scheduled run


class InitialRun(BaseModel):
    """Initial run configuration for an ingestor."""

    enabled: bool = False
    limit: int | None = 10  # Limit records for initial run
    since: datetime | None = None  # Optional: bootstrap from specific date


class IngestConfig(BaseModel):
    """Configuration for a named ingestor."""

    ingestor: str  # "geo", "ena", etc.
    config: AnyIngestorConfig
    schedule: IngestSchedule | None = None  # Optional: if set, runs on schedule
    initial_run: InitialRun | None = None  # Optional: if set, runs on startup
