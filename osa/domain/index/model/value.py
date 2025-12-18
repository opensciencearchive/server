"""Index configuration value objects."""

from typing import Annotated, Union

from pydantic import BaseModel, Field

from osa.infrastructure.index.vector.config import VectorBackendConfig

# Union of all backend configs (extend as new backends are added)
AnyBackendConfig = Annotated[
    Union[VectorBackendConfig],
    Field(discriminator=None),  # Could add discriminator when more backends exist
]


class IndexConfig(BaseModel):
    """Configuration for a named index."""

    backend: str  # "vector", "keyword", etc.
    config: AnyBackendConfig
