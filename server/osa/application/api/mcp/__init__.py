"""MCP Apps protocol adapter (#162).

A thin, domain-agnostic adapter exposing the node's published data to
MCP-Apps-capable hosts at ``/mcp``: model-visible view tools, app-only
interaction tools, and ``ui://osa/*`` widget resources. Every tool delegates
to the existing ``domain/data`` query handlers — this package adds protocol
translation only, never query logic.
"""
