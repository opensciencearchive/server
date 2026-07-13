"""DI seam for MCP tool dispatch (#162).

MCP callbacks run outside FastAPI's request scope, so the dispatcher opens a
``Scope.UOW`` container scope itself — exactly as background workers do — and
resolves the tool class inside it (Dishka injects the tool's query-handler
dependencies within the same scope). Identity is ``Anonymous``: the MCP
surface mirrors the public, unauthenticated ``/data/`` read surface, and every
delegated handler is ``__auth__ = public()``.
"""

from contextlib import AbstractAsyncContextManager

from dishka import AsyncContainer

from osa.domain.auth.model.identity import Anonymous, Identity
from osa.util.di.scope import Scope


def anonymous_uow(container: AsyncContainer) -> AbstractAsyncContextManager[AsyncContainer]:
    """One anonymous unit-of-work scope — one per MCP tool call."""
    return container(scope=Scope.UOW, context={Identity: Anonymous()})
