"""Serializer protocol — rows in, bytes out.

Serializers are stateless and have no I/O dependencies beyond stdlib (``json``,
``csv``, ``zlib``). They consume an async iterator of already-projected rows
(column→value mappings) and the column schema that fixes wire order, and yield
response bytes incrementally so streaming formats stay memory-bounded.

Note on ``next_cursor``: cursor derivation lives in the query service (matching
the proven discovery engine), not in the serializer. Paginated serializers
(JSON) receive the precomputed ``next_cursor`` to embed in the envelope;
streaming serializers (CSV, CSV.gz) ignore it.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from typing import Any, ClassVar, Protocol, runtime_checkable

from osa.domain.data.model.manifest import ColumnSpec


@runtime_checkable
class Serializer(Protocol):
    media_type: ClassVar[str]

    def stream(
        self,
        rows: AsyncIterator[Mapping[str, Any]],
        columns: Sequence[ColumnSpec],
        *,
        next_cursor: str | None = None,
    ) -> AsyncIterator[bytes]:
        """Render ``rows`` as response bytes, yielded incrementally."""
        ...
