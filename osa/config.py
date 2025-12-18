from pydantic_settings import BaseSettings

from osa.domain.index.model.value import IndexConfig
from osa.domain.ingest.model.value import IngestConfig


class Frontend(BaseSettings):
    url: str = "http://localhost:3000"


class Server(BaseSettings):
    name: str = "Open Science Archive"
    version: str = "0.0.1"  # TODO: better type?
    description: str = "An open platform for depositing scientific data"
    domain: str = "localhost"  # Node domain for SRN construction


class DatabaseConfig(BaseSettings):
    url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/osa"
    echo: bool = False


class Config(BaseSettings):
    server: Server = Server()
    frontend: Frontend = Frontend()
    database: DatabaseConfig = DatabaseConfig()
    indexes: dict[str, IndexConfig] = {}  # name -> IndexConfig
    ingestors: dict[str, IngestConfig] = {}  # name -> IngestConfig

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        env_nested_delimiter = "__"  # Allows OSA_DATABASE__URL override
