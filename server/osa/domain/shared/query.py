"""Query and QueryHandler base classes with authorization gate.

The gate wrapper and metaclass live once in :mod:`osa.domain.shared.handler`;
this module contributes only the read-side vocabulary: the ``Query`` DTO base
and an *unbound* result TypeVar.
"""

from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, ClassVar, Generic, TypeVar

from pydantic import BaseModel

from osa.domain.shared.handler import HandlerMeta, Result

if TYPE_CHECKING:
    from osa.domain.shared.authorization.gate import Gate

__all__ = ["Query", "QueryHandler", "Result"]


class Query(BaseModel): ...


C = TypeVar("C", bound=Query)
# Unbound: query results may be Result DTOs, domain read models (e.g. a
# catalog/manifest that IS the wire shape), or streaming reads. Result remains
# the conventional base for handler-specific DTOs.
R = TypeVar("R")


class QueryHandler(Generic[C, R], metaclass=HandlerMeta):
    """Base class for query handlers. Subclasses are automatically dataclasses.

    Declare __auth__ to enforce role-based access:
        class MyHandler(QueryHandler[MyQuery, MyResult]):
            __auth__ = at_least(Role.ADMIN)
            principal: Principal
    """

    __auth__: ClassVar[Gate]

    @abstractmethod
    async def run(self, cmd: C) -> R: ...
