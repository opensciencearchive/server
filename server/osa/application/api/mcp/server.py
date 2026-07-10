"""The MCP streamable-HTTP surface served at ``/mcp`` (#162).

Wires the low-level MCP ``Server`` to the tool registry and widget resources,
and exposes the surface as a raw ASGI endpoint plus a lifespan. Transport
choices (validated against the SDK's own stateless example and issue history):

- **exact-path Route, not Mount** — streamable HTTP is a single-endpoint
  transport, and a Starlette ``Mount("/mcp")`` would 307-redirect bare
  ``/mcp`` (python-sdk #951); an exact ``Route`` with this object as a raw
  ASGI endpoint serves the canonical URL directly.
- **stateless** — a fresh transport per request, no server-side session
  state; the surface scales horizontally exactly like the REST API.
- **json_response** — plain JSON bodies instead of SSE streams; the tools are
  single-shot reads, nothing streams incrementally.
- **DNS-rebinding protection explicitly off** — the low-level SDK path
  defaults to no Host/Origin validation *silently*; we make that choice
  explicit. This is a public, read-only, unauthenticated surface (the same
  posture as ``/data/``), and node deployments terminate TLS/virtual-hosting
  at the ingress, so Host allow-listing belongs there.

The session manager accepts requests only while :meth:`McpSurface.lifespan`
is entered (it owns the transport task group), and a manager is single-use —
so the surface is constructed per app instance and run from the app lifespan.

The server ``instructions`` are the node's rendered ``SKILL.md`` — captured
once at startup (initialize responses need a static string). Live grounding
always comes from ``list_datasets`` / ``describe_dataset``.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from typing import Any

import pydantic
from dishka import AsyncContainer
from mcp import types
from mcp.server.lowlevel import Server
from mcp.server.lowlevel.helper_types import ReadResourceContents
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from mcp.server.transport_security import TransportSecuritySettings
from pydantic import AnyUrl, BaseModel
from starlette.types import Receive, Scope, Send

from osa.application.api.mcp.meta import (
    MCP_APP_MIME,
    ResourceMeta,
    ResultMeta,
    ToolMeta,
)
from osa.application.api.mcp.resources import WIDGETS, WidgetDef, WidgetRegistry
from osa.application.api.mcp.tools import TOOLS, TOOLS_BY_NAME, Tool
from osa.application.api.mcp.uow import anonymous_uow
from osa.config import Config
from osa.domain.data.query.skill import GetSkillDocument, GetSkillDocumentHandler
from osa.domain.shared.error import DomainError, NotFoundError


class McpSurface:
    """The node's MCP Apps endpoint: a raw ASGI endpoint plus its lifespan."""

    def __init__(self, container: AsyncContainer, config: Config) -> None:
        self._container = container
        self.server = _build_server(container, config)
        self._session_manager = StreamableHTTPSessionManager(
            app=self.server,
            json_response=True,
            stateless=True,
            security_settings=TransportSecuritySettings(enable_dns_rebinding_protection=False),
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """ASGI entry point — registered as the exact-path ``/mcp`` route."""
        await self._session_manager.handle_request(scope, receive, send)

    @asynccontextmanager
    async def lifespan(self) -> AsyncIterator[None]:
        """Render instructions from the live catalog, then run the transport."""
        async with anonymous_uow(self._container) as scope:
            handler = await scope.get(GetSkillDocumentHandler)
            self.server.instructions = await handler.run(GetSkillDocument())
        async with self._session_manager.run():
            yield


def _build_server(container: AsyncContainer, config: Config) -> Server:
    server: Server = Server(name=config.name, version=config.version)
    registry = WidgetRegistry(bundle_dir=config.mcp.widget_bundle_dir)

    @server.list_tools()
    async def list_tools() -> list[types.Tool]:
        return [_tool_definition(cls) for cls in TOOLS]

    # Input is validated by the Pydantic argument models (same rules as the
    # REST boundary), so the SDK's jsonschema pre-validation is redundant.
    @server.call_tool(validate_input=False)
    async def call_tool(name: str, arguments: dict) -> types.CallToolResult:
        tool_cls = TOOLS_BY_NAME.get(name)
        if tool_cls is None:
            return _error_result(f"Unknown tool: {name!r}")
        try:
            args = tool_cls.spec.input_model.model_validate(arguments)
        except pydantic.ValidationError as exc:
            return _error_result(f"Invalid arguments for {name}: {exc}")
        try:
            async with anonymous_uow(container) as scope:
                handler = await scope.get(tool_cls.handler_type)
                payload = await tool_cls(handler).run(args)
        except DomainError as exc:
            # Business rejections (unknown schema/column, bad filter…) are the
            # model's/widget's to react to — a tool error, not a protocol one.
            return _error_result(str(exc))
        return _widget_result(payload, resource_uri=tool_cls.spec.resource_uri)

    @server.list_resources()
    async def list_resources() -> list[types.Resource]:
        return [_resource_definition(widget) for widget in WIDGETS]

    @server.read_resource()
    async def read_resource(uri: AnyUrl) -> list[ReadResourceContents]:
        try:
            return [registry.read(str(uri))]
        except NotFoundError as exc:
            # The SDK maps raised errors to a JSON-RPC error response.
            raise ValueError(str(exc)) from exc

    return server


# The SDK's `_meta` fields are alias-only (no populate_by_name), so the
# builders below construct via model_validate with the wire key.


def _tool_definition(cls: type[Tool[Any, Any, Any]]) -> types.Tool:
    return types.Tool.model_validate(
        {
            "name": cls.spec.name,
            "title": cls.spec.title,
            "description": cls.spec.description,
            "inputSchema": cls.spec.input_model.model_json_schema(),
            "_meta": ToolMeta.build(
                resource_uri=cls.spec.resource_uri, app_only=cls.spec.app_only
            ).dump(),
        }
    )


def _resource_definition(widget: WidgetDef) -> types.Resource:
    return types.Resource.model_validate(
        {
            "uri": widget.uri,
            "name": widget.uri,
            "title": widget.title,
            "description": widget.description,
            "mimeType": MCP_APP_MIME,
            "_meta": ResourceMeta().dump(),
        }
    )


def _widget_result(payload: BaseModel, *, resource_uri: str | None) -> types.CallToolResult:
    structured = payload.model_dump(mode="json", exclude_none=True)
    envelope: dict[str, Any] = {
        "content": [{"type": "text", "text": json.dumps(structured)}],
        "structuredContent": structured,
    }
    if resource_uri is not None:
        envelope["_meta"] = ResultMeta.build(resource_uri=resource_uri).dump()
    return types.CallToolResult.model_validate(envelope)


def _error_result(message: str) -> types.CallToolResult:
    return types.CallToolResult(
        content=[types.TextContent(type="text", text=message)],
        isError=True,
    )
