import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, Union

import yaml
from pydantic import BaseModel, Field, model_validator
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource
from typing_extensions import Self

from osa.cli.util.paths import OSAPaths
from osa.infrastructure.index.vector.config import VectorBackendConfig


# =============================================================================
# Index Configuration
# =============================================================================

# Union of all backend configs (extend as new backends are added)
AnyBackendConfig = Annotated[
    Union[VectorBackendConfig],
    Field(discriminator=None),  # Could add discriminator when more backends exist
]


class IndexConfig(BaseModel):
    """Configuration for an index."""

    name: str  # Unique name for this index (used in search API)
    backend: str  # "vector", "keyword", etc.
    config: AnyBackendConfig


# =============================================================================
# Source Configuration
# =============================================================================


class SourceSchedule(BaseModel):
    """Schedule configuration for a source."""

    cron: str  # Cron expression (e.g., "0 * * * *" for hourly)
    limit: int | None = None  # Optional limit per scheduled run


class InitialRun(BaseModel):
    """Initial run configuration for a source."""

    enabled: bool = False
    limit: int | None  # Required: number to limit, or null for no limit
    since: datetime | None = None  # Optional: bootstrap from specific date


class SourceConfig(BaseModel):
    """Configuration for a source.

    The `config` field is validated at runtime based on the source type,
    allowing external sources to define their own config schemas.
    """

    source: str  # "geo-entrez", etc. - matches entry point name
    config: dict[str, Any] = {}  # Validated at runtime by source's config_class
    schedule: SourceSchedule | None = None  # Optional: if set, runs on schedule
    initial_run: InitialRun | None = None  # Optional: if set, runs on startup

    @property
    def name(self) -> str:
        """The source name (same as source type for now)."""
        return self.source


# =============================================================================
# Application Configuration
# =============================================================================


class YamlConfigSettingsSource(PydanticBaseSettingsSource):
    """Load settings from YAML file specified by OSA_CONFIG_FILE env var."""

    def get_field_value(self, field: Any, field_name: str) -> tuple[Any, str, bool]:
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


class Frontend(BaseModel):
    """Frontend configuration (nested in Config, uses env_nested_delimiter)."""

    url: str = "http://localhost:3000"


class Server(BaseModel):
    """Server configuration (nested in Config, uses env_nested_delimiter)."""

    name: str = "Open Science Archive"
    version: str = "0.0.1"  # TODO: better type?
    description: str = "An open platform for depositing scientific data"
    domain: str = "localhost"  # Node domain for SRN construction


class DatabaseConfig(BaseModel):
    """Database configuration (nested in Config, uses env_nested_delimiter).

    The url field uses empty string as sentinel to indicate "derive from OSAPaths".
    When user doesn't override via OSA_DATABASE__URL, we compute the actual path
    in Config's model_validator.
    """

    url: str = ""  # Empty string = derive from paths; explicit value = use as-is
    echo: bool = False
    auto_migrate: bool = True  # Auto-migrate for SQLite, manual for PostgreSQL


class LoggingConfig(BaseModel):
    """Logging configuration (nested in Config, uses env_nested_delimiter)."""

    level: str = "DEBUG"  # Root log level (DEBUG for development)
    format: str = "%(asctime)s %(levelname)-8s [%(name)s] %(message)s"
    date_format: str = "%Y-%m-%d %H:%M:%S"

    @property
    def file(self) -> str | None:
        """Get log file path from OSA_LOG_FILE env var."""
        return os.environ.get("OSA_LOG_FILE")


class WorkerConfig(BaseModel):
    """Background worker configuration (nested in Config, uses env_nested_delimiter).

    Controls the behavior of the outbox polling worker that processes events.
    """

    poll_interval: float = 0.5  # Seconds between outbox polls
    batch_size: int = 100  # Maximum events to fetch per poll cycle


# =============================================================================
# Authentication Configuration
# =============================================================================


class OrcidConfig(BaseModel):
    """ORCiD OAuth configuration."""

    client_id: str = ""
    client_secret: str = ""
    sandbox: bool = True  # Use sandbox.orcid.org by default

    @property
    def base_url(self) -> str:
        """Get base URL for ORCiD API based on sandbox setting."""
        return "https://sandbox.orcid.org" if self.sandbox else "https://orcid.org"


class JwtConfig(BaseModel):
    """JWT configuration."""

    secret: str = ""  # Must be set in production
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 60  # 1 hour
    refresh_token_expire_days: int = 7


class AuthConfig(BaseModel):
    """Authentication configuration."""

    orcid: OrcidConfig = OrcidConfig()
    jwt: JwtConfig = JwtConfig()
    callback_url: str = ""  # Full callback URL (e.g., https://myarchive.org/api/v1/auth/callback)


class Config(BaseSettings):
    # These are BaseModel, so env_nested_delimiter handles their env vars
    server: Server = Server()
    frontend: Frontend = Frontend()
    database: DatabaseConfig = DatabaseConfig()
    logging: LoggingConfig = LoggingConfig()
    worker: WorkerConfig = WorkerConfig()  # Background worker settings
    auth: AuthConfig = AuthConfig()  # Authentication settings
    indexes: list[IndexConfig] = []  # list of index configs
    sources: list[SourceConfig] = []  # list of source configs

    model_config = {
        "env_prefix": "OSA_",
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "env_nested_delimiter": "__",  # Allows OSA_DATABASE__URL override
    }

    @model_validator(mode="after")
    def derive_database_url(self) -> Self:
        """Derive database URL from OSAPaths if not explicitly set.

        When database.url is empty (sentinel value), compute the path from OSAPaths.
        OSAPaths reads OSA_DATA_DIR directly from environment.
        This allows OSA_DATA_DIR to control the database location while still
        allowing explicit OSA_DATABASE__URL override.
        """
        if not self.database.url:
            # URL is sentinel (empty), derive from OSAPaths
            osa_paths = OSAPaths()
            self.database = DatabaseConfig(
                url=f"sqlite+aiosqlite:///{osa_paths.database_file}",
                echo=self.database.echo,
                auto_migrate=self.database.auto_migrate,
            )
        return self

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

    # Add file handler if log file specified (daemon mode)
    if config.file:
        log_path = Path(config.file).expanduser()
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path)
        file_handler.setLevel(config.level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    else:
        # Only add console handler when not using a log file.
        # When running as a daemon, stderr is redirected to the log file,
        # so we'd get double entries if we had both handlers.
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
