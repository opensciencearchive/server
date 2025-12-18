"""Base configuration for ingestors."""

from pydantic import BaseModel


class IngestorConfig(BaseModel):
    """Base configuration for ingestors.

    Extend this class for source-specific configuration.
    """

    pass
