"""Ingestor discovery via entry points."""

from __future__ import annotations

import logging
from importlib.metadata import entry_points
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ValidationError

from osa.sdk.ingest.ingestor import Ingestor

if TYPE_CHECKING:
    from osa.config import IngestConfig

logger = logging.getLogger(__name__)

ENTRY_POINT_GROUP = "osa.ingestors"


def discover_ingestors() -> dict[str, type[Ingestor]]:
    """Discover available ingestors via entry points.

    Scans for entry points in the 'osa.ingestors' group and loads
    the ingestor classes. Each entry point should point to a class
    that implements the Ingestor protocol.

    Returns:
        Dict mapping ingestor type names to their classes.

    Example pyproject.toml entry:
        [project.entry-points."osa.ingestors"]
        geo-entrez = "ingestors.geo_entrez:GEOEntrezIngestor"
    """
    ingestors: dict[str, type[Ingestor]] = {}
    eps = entry_points(group=ENTRY_POINT_GROUP)

    for ep in eps:
        try:
            cls = ep.load()
            _validate_ingestor_class(cls, ep.name)
            ingestors[ep.name] = cls
            logger.debug("Discovered ingestor: %s -> %s", ep.name, cls.__name__)
        except Exception as e:
            logger.warning("Failed to load ingestor '%s': %s", ep.name, e)

    return ingestors


def _validate_ingestor_class(cls: Any, name: str) -> None:
    """Validate that a class conforms to the Ingestor protocol.

    Args:
        cls: The class to validate (loaded from entry point).
        name: The entry point name for error messages.

    Raises:
        TypeError: If the class doesn't conform to the Ingestor protocol.
    """
    if not isinstance(cls, type):
        raise TypeError(f"Ingestor {name} must be a class, got {type(cls).__name__}")
    if not hasattr(cls, "name"):
        raise TypeError(f"Ingestor {name} missing 'name' class attribute")
    if not hasattr(cls, "config_class"):
        raise TypeError(f"Ingestor {name} missing 'config_class' class attribute")
    if not issubclass(cls.config_class, BaseModel):
        raise TypeError(f"Ingestor {name} config_class must be a Pydantic BaseModel")


def validate_ingestor_config(
    ingestor_cls: type[Ingestor],
    config_data: dict[str, Any],
) -> BaseModel:
    """Validate configuration data against an ingestor's config class.

    Args:
        ingestor_cls: The ingestor class with a config_class attribute.
        config_data: Raw configuration dictionary from user config.

    Returns:
        Validated config instance.

    Raises:
        ValidationError: If config data doesn't match the schema.
    """
    return ingestor_cls.config_class.model_validate(config_data)


class IngestorConfigError(Exception):
    """Raised when ingestor configuration validation fails."""

    def __init__(
        self,
        ingestor_name: str,
        name: str,
        validation_error: ValidationError,
    ) -> None:
        self.ingestor_name = ingestor_name
        self.name = name
        self.validation_error = validation_error
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        """Format a human-readable error message."""
        lines = [f"Invalid config for ingestor '{self.ingestor_name}' ({self.name}):"]
        for err in self.validation_error.errors():
            loc = ".".join(str(x) for x in err.get("loc", []))
            msg = err.get("msg", "Unknown error")
            lines.append(f"  - {loc}: {msg}")
        return "\n".join(lines)


def validate_ingestors_at_startup(ingestors_config: list[IngestConfig]) -> None:
    """Validate all ingestor configurations at application startup.

    Call this early in startup to fail fast with clear error messages
    if any ingestor configuration is invalid.

    Args:
        ingestors_config: List of IngestConfig from user config.

    Raises:
        IngestorConfigError: If any configuration is invalid.
        ValueError: If an unknown ingestor type is specified or duplicates exist.
    """
    available = discover_ingestors()
    validate_all_ingestor_configs(ingestors_config, available)


def validate_all_ingestor_configs(
    ingestors_config: list[IngestConfig],
    available_ingestors: dict[str, type[Ingestor]],
) -> dict[str, tuple[type[Ingestor], BaseModel]]:
    """Validate all ingestor configurations at startup.

    Args:
        ingestors_config: List of IngestConfig from user config.
        available_ingestors: Dict of ingestor type -> class from discovery.

    Returns:
        Dict of ingestor name -> (class, validated_config).

    Raises:
        IngestorConfigError: If any configuration is invalid.
        ValueError: If an unknown ingestor type is specified or duplicates exist.
    """
    validated: dict[str, tuple[type[Ingestor], BaseModel]] = {}

    for ing_config in ingestors_config:
        name = ing_config.ingestor

        # Check for duplicates
        if name in validated:
            raise ValueError(
                f"Duplicate ingestor '{name}'. Each ingestor type can only be configured once."
            )

        if name not in available_ingestors:
            available = ", ".join(sorted(available_ingestors.keys())) or "(none)"
            raise ValueError(f"Unknown ingestor type '{name}'. Available: {available}")

        ingestor_cls = available_ingestors[name]

        try:
            config = validate_ingestor_config(ingestor_cls, ing_config.config)
            validated[name] = (ingestor_cls, config)
        except ValidationError as e:
            raise IngestorConfigError(
                ingestor_name=name,
                name=name,
                validation_error=e,
            ) from e

    return validated
