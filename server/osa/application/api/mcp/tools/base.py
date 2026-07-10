"""Tool base class — the MCP analogue of a REST route (#162).

A tool is a class binding protocol identity to a domain query handler:

- ``spec: ClassVar[ToolSpec]`` — what hosts and models see in ``tools/list``
  (name, description, argument model, UI resource, visibility);
- ``handler_type: ClassVar`` — the ``domain/data`` query handler it delegates
  to. The dispatcher resolves it from an anonymous UOW scope and instantiates
  the tool with it, so Dishka only ever provides domain types — no DI
  registrations exist in this protocol layer;
- ``run(args)`` — a thin mapping from validated arguments to the handler's
  Query DTO, exactly like a route body.

The metaclass makes incompleteness an import-time error, not a first-call
surprise: a concrete tool without a ``ToolSpec`` or handler type fails to
import.
"""

from __future__ import annotations

from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import Any, ClassVar, Generic, TypeVar

from pydantic import BaseModel

from osa.domain.shared.query import QueryHandler

ArgsT = TypeVar("ArgsT", bound=BaseModel)
HandlerT = TypeVar("HandlerT", bound=QueryHandler[Any, Any])
PayloadT = TypeVar("PayloadT", bound=BaseModel)


@dataclass(frozen=True)
class ToolSpec:
    """A tool's protocol identity: what hosts and models see in ``tools/list``."""

    name: str
    title: str
    description: str
    input_model: type[BaseModel]
    resource_uri: str | None = None  # ui://osa/* widget rendered for results
    app_only: bool = False  # visibility excludes the model (widget-invoked only)


class _ToolMeta(ABCMeta):
    """Enforces the tool contract at import time for concrete subclasses."""

    def __new__(mcs, name: str, bases: tuple[type, ...], namespace: dict[str, Any]):
        cls = super().__new__(mcs, name, bases, namespace)
        if any(isinstance(b, mcs) for b in bases):
            if not isinstance(getattr(cls, "spec", None), ToolSpec):
                raise TypeError(f"Tool {name!r} must declare `spec: ClassVar[ToolSpec]`")
            handler_type = getattr(cls, "handler_type", None)
            if not (isinstance(handler_type, type) and issubclass(handler_type, QueryHandler)):
                raise TypeError(
                    f"Tool {name!r} must declare `handler_type: ClassVar` naming the "
                    "domain query handler it delegates to"
                )
        return cls


class Tool(Generic[ArgsT, HandlerT, PayloadT], metaclass=_ToolMeta):
    """Base for all MCP tools. ``run`` returns the ``structuredContent`` model."""

    spec: ClassVar[ToolSpec]
    handler_type: ClassVar[type[QueryHandler[Any, Any]]]

    def __init__(self, handler: HandlerT) -> None:
        self.handler = handler

    @abstractmethod
    async def run(self, args: ArgsT) -> PayloadT: ...
