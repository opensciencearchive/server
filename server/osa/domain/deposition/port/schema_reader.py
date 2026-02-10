from abc import abstractmethod
from typing import TYPE_CHECKING, Protocol

from osa.domain.shared.model.srn import SchemaSRN
from osa.domain.shared.port import Port

if TYPE_CHECKING:
    from osa.domain.semantics.model.schema import Schema


class SchemaReader(Port, Protocol):
    """Read-only cross-domain port for reading schemas from the deposition domain."""

    @abstractmethod
    async def get_schema(self, srn: SchemaSRN) -> "Schema | None": ...

    @abstractmethod
    async def schema_exists(self, srn: SchemaSRN) -> bool: ...
