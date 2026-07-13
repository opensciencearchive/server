"""MCP Apps ``_meta`` vocabulary — the single seam for the young spec (#162).

MCP Apps (SEP-1865, Jan 2026) rides on ``_meta`` extension blocks. The shapes
are fixed, so they are typed Pydantic models: ``ToolMeta`` (tool definition),
``ResultMeta`` (tool result), ``ResourceMeta`` (``ui://`` resource). Wire key
names — the ``ui`` envelope and camelCase members — exist only as field names
and serialization aliases here, so a spec rename is a one-module change.

The MCP SDK's ``_meta`` parameters take plain dicts; :meth:`MetaBlock.dump`
is the one narrowing point, called at the SDK boundary and nowhere else.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field

# Bundled-HTML MCP-App content type (the MVP content type — external-URL and
# remote-DOM resources are explicitly out of scope).
MCP_APP_MIME = "text/html;profile=mcp-app"


class Visibility(StrEnum):
    """Who may see/invoke a tool: the model, or widgets (the "app")."""

    MODEL = "model"
    APP = "app"


class MetaBlock(BaseModel):
    """Base for ``_meta`` envelopes; ``dump()`` renders the SDK-facing dict."""

    def dump(self) -> dict[str, Any]:
        return self.model_dump(by_alias=True, exclude_none=True)


class ToolUi(BaseModel):
    """``_meta.ui`` on a tool definition: visibility + optional widget binding.

    Hosts MUST keep tools whose visibility excludes ``model`` out of the
    model's tool list; widgets may still invoke them via the host.
    """

    visibility: list[Visibility]
    resource_uri: str | None = Field(default=None, serialization_alias="resourceUri")


class ToolMeta(MetaBlock):
    ui: ToolUi

    @classmethod
    def build(cls, *, resource_uri: str | None, app_only: bool) -> ToolMeta:
        visibility = [Visibility.APP] if app_only else [Visibility.MODEL, Visibility.APP]
        return cls(ui=ToolUi(visibility=visibility, resource_uri=resource_uri))


class ResultUi(BaseModel):
    """``_meta.ui`` on a tool result: which widget renders it."""

    resource_uri: str = Field(serialization_alias="resourceUri")


class ResultMeta(MetaBlock):
    ui: ResultUi

    @classmethod
    def build(cls, *, resource_uri: str) -> ResultMeta:
        return cls(ui=ResultUi(resource_uri=resource_uri))


class UiCsp(BaseModel):
    """Content-Security-Policy grants for a widget iframe.

    Both lists stay empty for the baseline widgets: everything is inlined and
    all data arrives via host-proxied tool calls, so the host applies
    default-deny (``default-src 'none'`` / ``connect-src 'none'``).
    """

    connect_domains: list[str] = Field(default_factory=list, serialization_alias="connectDomains")
    resource_domains: list[str] = Field(default_factory=list, serialization_alias="resourceDomains")


class ResourceUi(BaseModel):
    """``_meta.ui`` on a ``ui://`` resource: its sandbox CSP."""

    csp: UiCsp = Field(default_factory=UiCsp)


class ResourceMeta(MetaBlock):
    """Defaults to the default-deny CSP — ``ResourceMeta()`` is the baseline."""

    ui: ResourceUi = Field(default_factory=ResourceUi)
