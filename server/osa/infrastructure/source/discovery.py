"""Source discovery via entry points."""

from __future__ import annotations

import logging
from importlib.metadata import entry_points
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ValidationError

from osa.sdk.source.source import Source

if TYPE_CHECKING:
    from osa.config import SourceConfig

logger = logging.getLogger(__name__)

ENTRY_POINT_GROUP = "osa.sources"


def discover_sources() -> dict[str, type[Source]]:
    """Discover available sources via entry points.

    Scans for entry points in the 'osa.sources' group and loads
    the source classes. Each entry point should point to a class
    that implements the Source protocol.

    Returns:
        Dict mapping source type names to their classes.

    Example pyproject.toml entry:
        [project.entry-points."osa.sources"]
        geo-entrez = "sources.geo_entrez:GEOEntrezSource"
    """
    sources: dict[str, type[Source]] = {}
    eps = entry_points(group=ENTRY_POINT_GROUP)

    for ep in eps:
        try:
            cls = ep.load()
            _validate_source_class(cls, ep.name)
            sources[ep.name] = cls
            logger.debug("Discovered source: %s -> %s", ep.name, cls.__name__)
        except Exception as e:
            logger.warning("Failed to load source '%s': %s", ep.name, e)

    return sources


def _validate_source_class(cls: Any, name: str) -> None:
    """Validate that a class conforms to the Source protocol.

    Args:
        cls: The class to validate (loaded from entry point).
        name: The entry point name for error messages.

    Raises:
        TypeError: If the class doesn't conform to the Source protocol.
    """
    if not isinstance(cls, type):
        raise TypeError(f"Source {name} must be a class, got {type(cls).__name__}")
    if not hasattr(cls, "name"):
        raise TypeError(f"Source {name} missing 'name' class attribute")
    if not hasattr(cls, "config_class"):
        raise TypeError(f"Source {name} missing 'config_class' class attribute")
    if not issubclass(cls.config_class, BaseModel):
        raise TypeError(f"Source {name} config_class must be a Pydantic BaseModel")


def validate_source_config(
    source_cls: type[Source],
    config_data: dict[str, Any],
) -> BaseModel:
    """Validate configuration data against a source's config class.

    Args:
        source_cls: The source class with a config_class attribute.
        config_data: Raw configuration dictionary from user config.

    Returns:
        Validated config instance.

    Raises:
        ValidationError: If config data doesn't match the schema.
    """
    return source_cls.config_class.model_validate(config_data)


class SourceConfigError(Exception):
    """Raised when source configuration validation fails."""

    def __init__(
        self,
        source_name: str,
        name: str,
        validation_error: ValidationError,
    ) -> None:
        self.source_name = source_name
        self.name = name
        self.validation_error = validation_error
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        """Format a human-readable error message."""
        lines = [f"Invalid config for source '{self.source_name}' ({self.name}):"]
        for err in self.validation_error.errors():
            loc = ".".join(str(x) for x in err.get("loc", []))
            msg = err.get("msg", "Unknown error")
            lines.append(f"  - {loc}: {msg}")
        return "\n".join(lines)


def validate_sources_at_startup(sources_config: list[SourceConfig]) -> None:
    """Validate all source configurations at application startup.

    Call this early in startup to fail fast with clear error messages
    if any source configuration is invalid.

    Args:
        sources_config: List of SourceConfig from user config.

    Raises:
        SourceConfigError: If any configuration is invalid.
        ValueError: If an unknown source type is specified or duplicates exist.
    """
    available = discover_sources()
    validate_all_source_configs(sources_config, available)


def validate_all_source_configs(
    sources_config: list[SourceConfig],
    available_sources: dict[str, type[Source]],
) -> dict[str, tuple[type[Source], BaseModel]]:
    """Validate all source configurations at startup.

    Args:
        sources_config: List of SourceConfig from user config.
        available_sources: Dict of source type -> class from discovery.

    Returns:
        Dict of source name -> (class, validated_config).

    Raises:
        SourceConfigError: If any configuration is invalid.
        ValueError: If an unknown source type is specified or duplicates exist.
    """
    validated: dict[str, tuple[type[Source], BaseModel]] = {}

    for src_config in sources_config:
        name = src_config.source

        # Check for duplicates
        if name in validated:
            raise ValueError(
                f"Duplicate source '{name}'. Each source type can only be configured once."
            )

        if name not in available_sources:
            available = ", ".join(sorted(available_sources.keys())) or "(none)"
            raise ValueError(f"Unknown source type '{name}'. Available: {available}")

        source_cls = available_sources[name]

        try:
            config = validate_source_config(source_cls, src_config.config)
            validated[name] = (source_cls, config)
        except ValidationError as e:
            raise SourceConfigError(
                source_name=name,
                name=name,
                validation_error=e,
            ) from e

    return validated
