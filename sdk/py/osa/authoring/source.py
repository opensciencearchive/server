"""Source protocol for SDK convention packages — OCI container model."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any, ClassVar, Protocol

from pydantic import BaseModel

from osa.runtime.source_context import SourceContext
from osa.types.source import InitialRun, SourceRecord, SourceSchedule


class Source(Protocol):
    """Protocol for pluggable data sources running as OCI containers.

    Implement this in your convention package to define a source.
    Sources are built into Docker images and executed by the server.
    """

    name: ClassVar[str]
    schedule: ClassVar[SourceSchedule | None]
    initial_run: ClassVar[InitialRun | None]

    class RuntimeConfig(BaseModel): ...

    async def pull(
        self,
        *,
        ctx: SourceContext,
        since: datetime | None = None,
        limit: int | None = None,
        offset: int = 0,
        session: dict[str, Any] | None = None,
    ) -> AsyncIterator[SourceRecord]: ...
