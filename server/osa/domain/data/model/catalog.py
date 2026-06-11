"""Node catalog response envelope.

``GET /data`` returns the node's domain plus the published schemas, each with a
summary list of table resources (names + kinds only). Column-level detail comes
from the per-schema manifest at ``GET /data/{schema}``.
"""

from __future__ import annotations

from pydantic import BaseModel

from osa.domain.data.model.query_plan import TableKind


class TableResourceSummary(BaseModel):
    """Name + kind of an addressable table resource (no columns/counts)."""

    name: str
    kind: TableKind


class CatalogEntry(BaseModel):
    """One published schema in the node catalog."""

    id: str  # short schema id
    version: str  # SemVer
    srn: str  # full schema SRN
    table_resources: list[TableResourceSummary]


class NodeCatalog(BaseModel):
    """The node's published-schema catalog. Empty ``schemas`` is valid (200)."""

    node_domain: str
    schemas: list[CatalogEntry]
