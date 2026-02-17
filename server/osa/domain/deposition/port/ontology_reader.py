from abc import abstractmethod
from typing import TYPE_CHECKING, Protocol

from osa.domain.shared.model.srn import OntologySRN
from osa.domain.shared.port import Port

if TYPE_CHECKING:
    from osa.domain.semantics.model.ontology import Ontology


class OntologyReader(Port, Protocol):
    """Read-only cross-domain port for reading ontologies from the deposition domain."""

    @abstractmethod
    async def get_ontology(self, srn: OntologySRN) -> "Ontology | None": ...
