"""Ingest configuration value objects."""

from typing import Annotated, Union

from pydantic import BaseModel, Field

from osa.infrastructure.ingest.geo.config import GEOIngestorConfig

# Union of all ingestor configs (extend as new ingestors are added)
AnyIngestorConfig = Annotated[
    Union[GEOIngestorConfig],
    Field(discriminator=None),
]


class IngestConfig(BaseModel):
    """Configuration for a named ingestor."""

    ingestor: str  # "geo", "ena", etc.
    config: AnyIngestorConfig
