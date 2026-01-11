"""Base configuration for sources."""

from pydantic import BaseModel


class SourceConfig(BaseModel):
    """Base configuration for sources.

    Extend this class for source-specific configuration.
    """

    pass
