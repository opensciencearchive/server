"""MCP tool classes and the ordered registry (#162).

``TOOLS`` is the single source of truth for what the node serves: the
dispatcher and ``tools/list`` both derive from it, and each entry carries its
own ``ToolSpec`` and handler binding (enforced at import time by the tool
metaclass), so the registry cannot drift from behaviour.
"""

from typing import Any

from osa.application.api.mcp.tools.base import Tool, ToolSpec
from osa.application.api.mcp.tools.catalog import (
    DescribeDataset,
    ListDatasets,
    ShowFilterPanel,
    ShowRecord,
)
from osa.application.api.mcp.tools.table import (
    FetchPage,
    SampleValues,
    ShowChart,
    ShowTable,
)

TOOLS: tuple[type[Tool[Any, Any, Any]], ...] = (
    ListDatasets,
    DescribeDataset,
    ShowTable,
    ShowChart,
    ShowRecord,
    ShowFilterPanel,
    FetchPage,
    SampleValues,
)

TOOLS_BY_NAME: dict[str, type[Tool[Any, Any, Any]]] = {cls.spec.name: cls for cls in TOOLS}

__all__ = [
    "TOOLS",
    "TOOLS_BY_NAME",
    "Tool",
    "ToolSpec",
    "ListDatasets",
    "DescribeDataset",
    "ShowTable",
    "ShowChart",
    "ShowRecord",
    "ShowFilterPanel",
    "FetchPage",
    "SampleValues",
]
