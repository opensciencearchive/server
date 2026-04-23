from abc import abstractmethod
from typing import TYPE_CHECKING, List, Protocol

from osa.domain.shared.model.srn import SchemaId
from osa.domain.shared.port import Port

if TYPE_CHECKING:
    from osa.domain.semantics.model.schema import Schema


class SchemaRepository(Port, Protocol):
    @abstractmethod
    async def save(self, schema: "Schema") -> None: ...

    @abstractmethod
    async def get(self, schema_id: SchemaId) -> "Schema | None": ...

    @abstractmethod
    async def list(
        self, *, limit: int | None = None, offset: int | None = None
    ) -> "List[Schema]": ...

    @abstractmethod
    async def exists(self, schema_id: SchemaId) -> bool: ...
