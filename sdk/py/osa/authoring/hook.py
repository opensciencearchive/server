"""Unified @hook decorator — replaces @validator and @transform."""

from __future__ import annotations

from collections.abc import Callable

from osa._registry import register


def hook[F: Callable](fn: F) -> F:
    """Decorator that marks a function as an OSA hook.

    Registers the function in the global hook registry and
    introspects type hints to extract the schema type, output type,
    and cardinality (``-> T`` = one, ``-> list[T]`` = many).
    """
    register(fn, "hook")
    return fn
