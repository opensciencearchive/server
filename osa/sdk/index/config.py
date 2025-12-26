"""Base configuration for storage backends."""

from pydantic import BaseModel


class BackendConfig(BaseModel):
    """Base configuration for storage backends.

    Extend this class for vendor-specific configuration.
    """

    pass
