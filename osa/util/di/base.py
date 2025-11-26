from __future__ import annotations
from typing import ClassVar, Literal, Type

from dishka import Provider


Component = Literal[
    "assurance",
    "curation",
    "deposition",
    "export",
    "record",
    "schema",
    "search",
    "validation",
]

PROVIDERS: list[Type[ProviderBase]] = []


class ProviderBase(Provider):
    """Base for all DI providers with unified metadata.

    Attributes:
        __mock_component__: Component name (for mockable components, None for concrete providers)
        __is_mock__: Whether this is a mock implementation
    """

    __mock_component__: ClassVar[Component | None] = None
    __is_mock__: ClassVar[bool] = False


def get_provider(
    base: Type[ProviderBase], use_mock: bool = False
) -> Type[ProviderBase]:
    """Get appropriate provider class.

    Automatically determines if provider is mockable by checking for subclasses.

    - No subclasses: Concrete provider, use directly
    - Has subclasses: Mockable component, select by __is_mock__ flag

    Args:
        base: Provider base class
        use_mock: Whether to use mock implementation

    Returns:
        Provider class (not instantiated)

    Raises:
        ValueError: If requested implementation not found
    """
    subclasses = base.__subclasses__()

    if not subclasses:
        # Concrete provider - no implementations, use as-is
        return base

    # Has subclasses - it's a mockable component
    # Find implementation by __is_mock__ flag
    impl = next(
        (c for c in subclasses if getattr(c, "__is_mock__", False) == use_mock),
        None,
    )

    if not impl:
        kind = "mock" if use_mock else "production"
        component_name = getattr(base, "__mock_component__", base.__name__)
        raise ValueError(f"No {kind} implementation for {component_name}")

    return impl
