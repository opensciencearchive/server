from abc import abstractmethod
from typing import TYPE_CHECKING, List, Protocol

from osa.domain.shared.model.srn import OntologySRN
from osa.domain.shared.port import Port

if TYPE_CHECKING:
    from osa.domain.semantics.model.ontology import Ontology


class OntologyRepository(Port, Protocol):
    @abstractmethod
    async def save(self, ontology: "Ontology") -> None: ...

    @abstractmethod
    async def get(self, srn: OntologySRN) -> "Ontology | None": ...

    @abstractmethod
    async def list(
        self, *, limit: int | None = None, offset: int | None = None
    ) -> "List[Ontology]": ...

    @abstractmethod
    async def exists(self, srn: OntologySRN) -> bool: ...
