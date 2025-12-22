import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, Union

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource

from osa.infrastructure.index.vector.config import VectorBackendConfig
from osa.infrastructure.ingest.geo.config import GEOIngestorConfig


# =============================================================================
# Index Configuration
# =============================================================================

# Union of all backend configs (extend as new backends are added)
AnyBackendConfig = Annotated[
    Union[VectorBackendConfig],
    Field(discriminator=None),  # Could add discriminator when more backends exist
]


class IndexConfig(BaseModel):
    """Configuration for a named index."""

    backend: str  # "vector", "keyword", etc.
    config: AnyBackendConfig


# =============================================================================
# Ingest Configuration
# =============================================================================

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


# =============================================================================
# Application Configuration
# =============================================================================


class YamlConfigSettingsSource(PydanticBaseSettingsSource):
    """Load settings from YAML file specified by OSA_CONFIG_FILE env var."""

    def get_field_value(
        self, field: Any, field_name: str
    ) -> tuple[Any, str, bool]:
        """Get the value for a field from the YAML config."""
        yaml_data = self._load_yaml_config()
        field_value = yaml_data.get(field_name)
        return field_value, field_name, False

    def __call__(self) -> dict[str, Any]:
        """Return all settings from YAML file."""
        return self._load_yaml_config()

    def _load_yaml_config(self) -> dict[str, Any]:
        """Load config from YAML file if specified."""
        config_file = os.environ.get("OSA_CONFIG_FILE")
        if config_file:
            path = Path(config_file)
            if path.exists():
                return yaml.safe_load(path.read_text()) or {}
        return {}


class Frontend(BaseSettings):
    url: str = "http://localhost:3000"


class Server(BaseSettings):
    name: str = "Open Science Archive"
    version: str = "0.0.1"  # TODO: better type?
    description: str = "An open platform for depositing scientific data"
    domain: str = "localhost"  # Node domain for SRN construction


class DatabaseConfig(BaseSettings):
    url: str = "sqlite+aiosqlite:///~/.osa/osa.db"  # Default to SQLite for local operation
    echo: bool = False
    auto_migrate: bool = True  # Auto-migrate for SQLite, manual for PostgreSQL


class LoggingConfig(BaseSettings):
    level: str = "DEBUG"  # Root log level (DEBUG for development)
    format: str = "%(asctime)s %(levelname)-8s [%(name)s] %(message)s"
    date_format: str = "%Y-%m-%d %H:%M:%S"

    @property
    def file(self) -> str | None:
        """Get log file path from OSA_LOG_FILE env var."""
        return os.environ.get("OSA_LOG_FILE")


class Config(BaseSettings):
    server: Server = Server()
    frontend: Frontend = Frontend()
    database: DatabaseConfig = DatabaseConfig()
    logging: LoggingConfig = LoggingConfig()
    indexes: dict[str, IndexConfig] = {}  # name -> IndexConfig
    ingestors: dict[str, IngestConfig] = {}  # name -> IngestConfig

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        env_nested_delimiter = "__"  # Allows OSA_DATABASE__URL override

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Customize settings sources to include YAML config.

        Priority (highest to lowest):
        1. init_settings - values passed to Config()
        2. env_settings - environment variables
        3. dotenv_settings - .env file
        4. yaml_settings - OSA_CONFIG_FILE yaml
        5. file_secret_settings - secrets from files
        """
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            YamlConfigSettingsSource(settings_cls),
            file_secret_settings,
        )


def configure_logging(config: LoggingConfig) -> None:
    """Configure Python logging based on config.

    Should be called early in application startup, before other modules
    are imported to ensure all loggers pick up the configuration.
    """
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(config.level)

    # Remove existing handlers to avoid duplicates
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    formatter = logging.Formatter(config.format, datefmt=config.date_format)

    # Add file handler if log file specified
    if config.file:
        log_path = Path(config.file).expanduser()
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path)
        file_handler.setLevel(config.level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    # Always add console handler for stderr
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(config.level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Reduce noise from third-party libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("aiosqlite").setLevel(logging.WARNING)
    logging.getLogger("apscheduler").setLevel(logging.WARNING)  # Suppress job completion spam
    logging.getLogger("filelock").setLevel(logging.WARNING)  # Suppress huggingface lock spam
    logging.getLogger("chromadb").setLevel(logging.INFO)  # Suppress chromadb debug noise
    logging.getLogger("sentence_transformers").setLevel(logging.INFO)

    logging.debug("Logging configured: level=%s, file=%s", config.level, config.file)
