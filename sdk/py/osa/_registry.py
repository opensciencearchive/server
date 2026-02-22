"""Global hook and convention registry for @hook decorator and convention()."""

from __future__ import annotations

import typing
from collections.abc import Callable
from dataclasses import dataclass, field


@dataclass
class HookInfo:
    """Metadata extracted from a decorated hook function."""

    fn: Callable
    name: str
    hook_type: str
    schema_type: type
    return_type: type | None = None
    output_type: type | None = None
    cardinality: str = "one"
    dependencies: dict[str, type] = field(default_factory=dict)


@dataclass
class SourceInfo:
    """Metadata from a registered source class."""

    source_cls: type
    name: str
    schedule: object | None = None
    initial_run: object | None = None


@dataclass
class ConventionInfo:
    """Metadata from a convention() declaration."""

    title: str
    version: str
    schema_type: type
    file_requirements: dict
    hooks: list[Callable]
    source_type: type | None = None
    source_info: SourceInfo | None = None


_hooks: list[HookInfo] = []
_conventions: list[ConventionInfo] = []
_sources: list[SourceInfo] = []


def clear() -> None:
    """Remove all registered hooks, conventions, and sources. Used in tests."""
    _hooks.clear()
    _conventions.clear()
    _sources.clear()


def register_source(source_cls: type) -> SourceInfo:
    """Register a source class and return its SourceInfo."""
    name = getattr(source_cls, "name", source_cls.__name__)
    schedule = getattr(source_cls, "schedule", None)
    initial_run = getattr(source_cls, "initial_run", None)
    info = SourceInfo(
        source_cls=source_cls,
        name=name,
        schedule=schedule,
        initial_run=initial_run,
    )
    _sources.append(info)
    return info


def _extract_hook_info(fn: Callable, hook_type: str) -> HookInfo:
    """Introspect a hook function's type hints to extract metadata."""
    hints = typing.get_type_hints(fn)

    schema_type: type | None = None
    return_type: type | None = None
    dependencies: dict[str, type] = {}

    for param_name, hint in hints.items():
        if param_name == "return":
            return_type = hint
            continue

        origin = typing.get_origin(hint)
        if origin is not None:
            args = typing.get_args(hint)
            if getattr(origin, "__name__", "") == "Record" and args:
                schema_type = args[0]
                continue

        # Any other typed parameter is a dependency
        dependencies[param_name] = hint

    if schema_type is None:
        msg = f"Hook {fn.__name__} must have a Record[T] parameter"
        raise TypeError(msg)

    # Determine output_type and cardinality from return type
    output_type: type | None = None
    cardinality = "one"
    if return_type is not None:
        if typing.get_origin(return_type) is list:
            cardinality = "many"
            args = typing.get_args(return_type)
            output_type = args[0] if args else None
        else:
            cardinality = "one"
            output_type = return_type

    return HookInfo(
        fn=fn,
        name=fn.__name__,
        hook_type=hook_type,
        schema_type=schema_type,
        return_type=return_type,
        output_type=output_type,
        cardinality=cardinality,
        dependencies=dependencies,
    )


def register(fn: Callable, hook_type: str) -> None:
    """Register a decorated function as a hook."""
    info = _extract_hook_info(fn, hook_type)
    _hooks.append(info)
